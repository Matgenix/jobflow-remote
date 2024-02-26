from __future__ import annotations

import datetime
import inspect
import io
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Callable

import orjson
from jobflow.core.job import Job
from jobflow.core.store import JobStore
from maggma.core import Sort, Store
from maggma.utils import to_dt
from monty.io import zopen

# from maggma.stores.mongolike import JSONStore
from monty.json import jsanitize

from jobflow_remote.jobs.data import RemoteError
from jobflow_remote.utils.data import uuid_to_path

JOB_INIT_ARGS = {k for k in inspect.signature(Job).parameters.keys() if k != "kwargs"}
"""A set of the arguments of the Job constructor which
can be used to detect additional custom arguments
"""


def get_job_path(
    job_id: str, index: int | None, base_path: str | Path | None = None
) -> str:
    if base_path:
        base_path = Path(base_path)
    else:
        base_path = Path()

    relative_path = uuid_to_path(job_id, index)
    return str(base_path / relative_path)


def get_remote_in_file(job, remote_store):
    d = jsanitize(
        {"job": job, "store": remote_store},
        strict=True,
        allow_bson=True,
        enum_values=True,
    )
    return io.BytesIO(orjson.dumps(d, default=default_orjson_serializer))


def default_orjson_serializer(obj: Any) -> Any:
    type_obj = type(obj)
    if type_obj != float and issubclass(type_obj, float):
        return float(obj)
    raise TypeError


def get_remote_store(
    store: JobStore, launch_dir: str | Path, add_orjson_serializer: bool = True
) -> JobStore:
    serialization_default = None
    if add_orjson_serializer:
        serialization_default = default_orjson_serializer

    docs_store = MinimalJSONStore(
        os.path.join(launch_dir, "remote_job_data.json.gz"),
        serialization_default=serialization_default,
    )
    additional_stores = {}
    for k in store.additional_stores.keys():
        additional_stores[k] = MinimalJSONStore(
            os.path.join(launch_dir, f"additional_store_{k}.json.gz"),
            serialization_default=serialization_default,
        )
    remote_store = JobStore(
        docs_store=docs_store,
        additional_stores=additional_stores,
        save=store.save,
        load=store.load,
    )

    return remote_store


def get_remote_store_filenames(store: JobStore) -> list[str]:
    filenames = ["remote_job_data.json.gz"]
    for k in store.additional_stores.keys():
        filenames.append(f"additional_store_{k}.json.gz")

    return filenames


def update_store(store: JobStore, remote_store: JobStore, db_id: int):
    try:
        store.connect()
        remote_store.connect()

        additional_stores = set(store.additional_stores.keys())
        additional_remote_stores = set(remote_store.additional_stores.keys())

        # This checks that the additional stores in the two stores match correctly.
        # It should not happen if not because of a bug, so the check could maybe be
        # removed
        if additional_stores ^ additional_remote_stores:
            raise ValueError(
                f"The additional stores in the local and remote JobStore do not "
                f"match: {additional_stores ^ additional_remote_stores}"
            )

        # copy the data store by store, not using directly the JobStore.
        # This avoids the need to deserialize the store content and the "save"
        # argument.
        for add_store_name, remote_add_store in remote_store.additional_stores.items():
            add_store = store.additional_stores[add_store_name]

            for d in remote_add_store.query():
                data = dict(d)
                data.pop("_id", None)
                add_store.update(data)
        main_docs_list = list(remote_store.docs_store.query({}))
        if len(main_docs_list) > 1:
            raise RuntimeError(
                "The downloaded output store contains more than one document"
            )
        main_doc = main_docs_list[0]
        main_doc.pop("_id", None)
        # Set the db_id here and not directly in the Job's metadata to prevent
        # it from being propagated to its children/replacements.
        if "db_id" not in main_doc["metadata"]:
            main_doc["metadata"]["db_id"] = db_id
        store.docs_store.update(main_doc, key=["uuid", "index"])
    finally:
        try:
            store.close()
        except Exception:
            logging.error(f"error while closing the store {store}", exc_info=True)
        try:
            remote_store.close()
        except Exception:
            logging.error(
                f"error while closing the remote store {remote_store}", exc_info=True
            )


def resolve_job_dict_args(job_dict: dict, store: JobStore) -> dict:
    """
    Resolve the references in a serialized Job.

    Similar to Job.resolve_args, but without the need to deserialize the Job.
    The references are resolved inplace.

    Parameters
    ----------
    job_dict
        The serialized version of a Job.
    store
        The JobStore from where the references should be resolved.

    Returns
    -------
        The updated version of the input dictionary with references resolved.
    """
    from jobflow.core.reference import OnMissing, find_and_resolve_references

    on_missing = OnMissing(job_dict["config"]["on_missing_references"])
    cache: dict[str, Any] = {}
    resolved_args = find_and_resolve_references(
        job_dict["function_args"], store, cache=cache, on_missing=on_missing
    )
    resolved_kwargs = find_and_resolve_references(
        job_dict["function_kwargs"], store, cache=cache, on_missing=on_missing
    )
    resolved_args = tuple(resolved_args)

    # substitution is in place
    job_dict["function_args"] = resolved_args
    job_dict["function_kwargs"] = resolved_kwargs

    missing_stores = check_additional_stores(job_dict, store)
    if missing_stores:
        raise RemoteError(
            f"Additional stores {missing_stores!r} are not configured for this project.",
            no_retry=True,
        )

    return job_dict


def check_additional_stores(job: dict | Job, store: JobStore) -> list[str]:
    """
    Check if all the required additional stores have been defined in
    the output JobStore. If some are missing return the names of the missing Stores.

    Parameters
    ----------
    job
        A Job or its serialized version.
    store
        The JobStore from where the references should be resolved.

    Returns
    -------
        The list of names of the missing additional stores.
        An empty list if no store is missing.
    """
    if isinstance(job, dict):
        additional_store_names = set(job.keys()) - JOB_INIT_ARGS
    else:
        # TODO expose the _kwargs attribute in jobflow through an
        # "additional_stores" property
        additional_store_names = set(job._kwargs.keys())
    missing_stores = []
    for store_name in additional_store_names:
        # Exclude MSON fields
        if store_name.startswith("@"):
            continue
        if store_name not in store.additional_stores:
            missing_stores.append(store_name)
    return missing_stores


class MinimalFileStore(Store):
    """
    A Minimal Store for access to a single file.
    Only methods required by jobflow-remote are implemented.
    """

    @property
    def _collection(self):
        raise NotImplementedError

    @property
    def name(self) -> str:
        return f"json://{self.path}"

    def close(self):
        pass

    def count(self, criteria: dict | None = None) -> int:
        return len(self.data)

    def query(
        self,
        criteria: dict | None = None,
        properties: dict | list | None = None,
        sort: dict[str, Sort | int] | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[dict]:
        if criteria or properties or sort or skip or sort:
            raise NotImplementedError(
                "Query only implemented to return the whole set of docs"
            )

        return iter(self.data)

    def ensure_index(self, key: str, unique: bool = False) -> bool:
        raise NotImplementedError

    def groupby(
        self,
        keys: list[str] | str,
        criteria: dict | None = None,
        properties: dict | list | None = None,
        sort: dict[str, Sort | int] | None = None,
        skip: int = 0,
        limit: int = 0,
    ) -> Iterator[tuple[dict, list[dict]]]:
        raise NotImplementedError

    def __init__(
        self,
        path: str,
        serialization_option: int | None = None,
        serialization_default: Callable[[Any], Any] | None = None,
        **kwargs,
    ):
        """
        Args:
            path: paths for json files to turn into a Store
        """

        self.path = path

        self.kwargs = kwargs

        self.default_sort = None
        self.serialization_option = serialization_option
        self.serialization_default = serialization_default
        self.data: list[dict] = []

        super().__init__(**kwargs)

    def connect(self, force_reset: bool = False):
        """
        Loads the files into the collection in memory.
        """

        # create the .json file if it does not exist
        if not Path(self.path).exists():
            self.update_file()
        else:
            self.data = self.read_file()

    def update(self, docs: list[dict] | dict, key: list | str | None = None):
        """
        Update documents into the Store.

        For a file-writable JSONStore, the json file is updated.

        Args:
            docs: the document or list of documents to update
            key: field name(s) to determine uniqueness for a
                 document, can be a list of multiple fields,
                 a single field, or None if the Store's key
                 field is to be used
        """
        if not isinstance(docs, (list, tuple)):
            docs = [docs]

        self.data.extend(docs)

        self.update_file()

    def update_file(self):
        raise NotImplementedError

    def read_file(self) -> list:
        raise NotImplementedError

    def remove_docs(self, criteria: dict):
        """
        Remove docs matching the query dictionary.

        For a file-writable JSONStore, the json file is updated.

        Args:
            criteria: query dictionary to match
        """
        raise NotImplementedError

    def __hash__(self):
        return hash((*self.path, self.last_updated_field))

    def __eq__(self, other: object) -> bool:
        """
        Check equality for JSONStore.

        Args:
            other: other JSONStore to compare with
        """
        if not isinstance(other, self.__class__):
            return False

        fields = ["path", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class MinimalJSONStore(MinimalFileStore):

    def __init__(
        self,
        path: str,
        serialization_option: int | None = None,
        serialization_default: Callable[[Any], Any] | None = None,
        **kwargs,
    ):
        """
        Args:
            path: paths for json files to turn into a Store
            serialization_option:
                option that will be passed to the orjson.dump when saving to the
                json file.
            serialization_default:
                default that will be passed to the orjson.dump when saving to the
                json file.
        """
        self.serialization_option = serialization_option
        self.serialization_default = serialization_default

        super().__init__(path=path, **kwargs)

    def update_file(self):
        """
        Updates the json file when a write-like operation is performed.
        """
        with zopen(self.path, "wb") as f:
            for d in self.data:
                d.pop("_id", None)
            bytesdata = orjson.dumps(
                self.data,
                option=self.serialization_option,
                default=self.serialization_default,
            )
            f.write(bytesdata)

    def read_file(self) -> list:
        """
        Helper method to read the contents of a JSON file and generate
        a list of docs.
        """
        with zopen(self.path, "rb") as f:
            data = f.read()
            objects = orjson.loads(data)
            objects = [objects] if not isinstance(objects, list) else objects
            # datetime objects deserialize to str. Try to convert the last_updated
            # field back to datetime.
            # # TODO - there may still be problems caused if a JSONStore is init'ed from
            # documents that don't contain a last_updated field
            # See Store.last_updated in store.py.
            for obj in objects:
                if obj.get(self.last_updated_field):
                    obj[self.last_updated_field] = to_dt(obj[self.last_updated_field])

        return objects


def decode_datetime(obj):
    if "__datetime__" in obj:
        obj = datetime.datetime.strptime(obj["as_str"], "%Y%m%dT%H:%M:%S.%f")
    return obj


def encode_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return {"__datetime__": True, "as_str": obj.strftime("%Y%m%dT%H:%M:%S.%f")}
    return obj


class MinimalMsgpackStore(MinimalFileStore):

    def update_file(self):
        """
        Updates the msgpack file when a write-like operation is performed.
        """
        import msgpack

        with zopen(self.path, "wb") as f:
            msgpack.pack(self.data, f, default=encode_datetime, use_bin_type=True)

    def read_file(self) -> list:
        """
        Helper method to read the contents of a msgpack file and generate
        a list of docs.
        """
        import msgpack

        with zopen(self.path, "rb") as f:
            objects = msgpack.unpack(f, object_hook=decode_datetime, raw=False)
            objects = [objects] if not isinstance(objects, list) else objects
            # datetime objects deserialize to str. Try to convert the last_updated
            # field back to datetime.
            # # TODO - there may still be problems caused if a JSONStore is init'ed from
            # documents that don't contain a last_updated field
            # See Store.last_updated in store.py.
            for obj in objects:
                if obj.get(self.last_updated_field):
                    obj[self.last_updated_field] = to_dt(obj[self.last_updated_field])

        return objects
