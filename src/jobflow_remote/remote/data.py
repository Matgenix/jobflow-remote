from __future__ import annotations

import datetime
import inspect
import io
import logging
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import orjson
from jobflow.core.job import Job
from jobflow.core.store import JobStore
from maggma.core import Sort, Store
from maggma.stores import JSONStore
from maggma.utils import to_dt
from monty.io import zopen

# from maggma.stores.mongolike import JSONStore
from monty.json import MontyDecoder, jsanitize

from jobflow_remote.jobs.data import RemoteError
from jobflow_remote.utils.data import uuid_to_path

JOB_INIT_ARGS = {k for k in inspect.signature(Job).parameters if k != "kwargs"}
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
    store: JobStore, work_dir: str | Path, config_dict: dict | None
) -> JobStore:
    docs_store = get_single_store(
        config_dict=config_dict, file_name="remote_job_data", dir_path=work_dir
    )

    additional_stores = {}
    for k in store.additional_stores:
        additional_stores[k] = get_single_store(
            config_dict=config_dict,
            file_name=f"additional_store_{k}",
            dir_path=work_dir,
        )

    remote_store = JobStore(
        docs_store=docs_store,
        additional_stores=additional_stores,
        save=store.save,
        load=store.load,
    )

    return remote_store


default_remote_store = {"store": "maggma_json", "zip": False}


def get_single_store(
    config_dict: dict | None, file_name: str, dir_path: str | Path
) -> Store:
    config_dict = config_dict or default_remote_store

    store_type = config_dict.get("store", default_remote_store["store"])
    total_file_name = get_single_store_file_name(config_dict, file_name)
    file_path = os.path.join(dir_path, total_file_name)
    if store_type == "maggma_json":
        return StdJSONStore(file_path)
    elif store_type == "orjson":
        return MinimalORJSONStore(file_path)
    elif store_type == "msgspec_json":
        return MinimalMsgspecJSONStore(file_path)
    elif store_type == "msgpack":
        return MinimalMsgpackStore(file_path)
    elif isinstance(store_type, dict):
        store_type = dict(store_type)
        store_type["path"] = file_path
        store = MontyDecoder().process_decoded(store_type)
        if not isinstance(store, Store):
            raise ValueError(
                f"Could not instantiate a proper store from remote config dict {store_type}"
            )
    else:
        raise ValueError(f"remote store type not supported: {store_type}")


def get_single_store_file_name(config_dict: dict | None, file_name: str) -> str:
    config_dict = config_dict or default_remote_store
    store_type = config_dict.get("store", default_remote_store["store"])

    if isinstance(store_type, str) and "json" in store_type:
        ext = "json"
    elif isinstance(store_type, str) and "msgpack" in store_type:
        ext = "msgpack"
    else:
        ext = config_dict.get("extension")  # type: ignore
    if not ext:
        raise ValueError(
            f"Could not determine extension for remote store config dict: {config_dict}"
        )
    total_file_name = f"{file_name}.{ext}"
    if config_dict.get("zip", False):
        total_file_name += ".gz"
    return total_file_name


def get_remote_store_filenames(store: JobStore, config_dict: dict | None) -> list[str]:
    filenames = [
        get_single_store_file_name(config_dict=config_dict, file_name="remote_job_data")
    ]
    for k in store.additional_stores:
        filenames.append(
            get_single_store_file_name(
                config_dict=config_dict, file_name=f"additional_store_{k}"
            )
        )

    return filenames


def get_store_file_paths(store: JobStore) -> list[str]:
    def get_single_path(base_store: Store):
        paths = getattr(base_store, "paths", None)
        if paths:
            return paths[0]
        path = getattr(base_store, "path", None)
        if not path:
            raise RuntimeError(f"Could not determine the path for {base_store}")
        return path

    store_paths = [get_single_path(store.docs_store)]
    store_paths.extend(get_single_path(s) for s in store.additional_stores.values())
    return store_paths


def update_store(store: JobStore, remote_store: JobStore, db_id: int):
    try:
        store.connect()
        remote_store.connect()

        additional_stores = set(store.additional_stores)
        additional_remote_stores = set(remote_store.additional_stores)

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
        additional_store_names = set(job) - JOB_INIT_ARGS
    else:
        # TODO expose the _kwargs attribute in jobflow through an
        # "additional_stores" property
        additional_store_names = set(job._kwargs)
    missing_stores = []
    for store_name in additional_store_names:
        # Exclude MSON fields
        if store_name.startswith("@"):
            continue
        if store_name not in store.additional_stores:
            missing_stores.append(store_name)
    return missing_stores


class StdJSONStore(JSONStore):
    """
    Simple subclass of the JSONStore defining the serialization_default
    that cannot be dumped to json
    """

    def __init__(self, paths, **kwargs):
        super().__init__(
            paths=paths,
            serialization_default=default_orjson_serializer,
            read_only=False,
            **kwargs,
        )


class MinimalFileStore(Store):
    """
    A Minimal Store for access to a single file.
    Only methods required by jobflow-remote are implemented.
    """

    @property
    def _collection(self):
        raise NotImplementedError

    def close(self):
        self.update_file()

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
        **kwargs,
    ):
        """
        Args:
            path: paths for json files to turn into a Store
        """
        self.path = path

        self.kwargs = kwargs

        self.default_sort = None
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
        if not isinstance(other, type(self)):
            return False

        fields = ["path", "last_updated_field"]
        return all(getattr(self, f) == getattr(other, f) for f in fields)


class MinimalORJSONStore(MinimalFileStore):
    @property
    def name(self) -> str:
        return f"json://{self.path}"

    def update_file(self):
        """
        Updates the json file when a write-like operation is performed.
        """
        with zopen(self.path, "wb") as f:
            for d in self.data:
                d.pop("_id", None)
            bytesdata = orjson.dumps(
                self.data,
                default=default_orjson_serializer,
            )
            f.write(bytesdata)

    def read_file(self) -> list:
        """
        Helper method to read the contents of a JSON file and generate
        a list of docs.
        """
        with zopen(self.path, "rb") as f:
            data = f.read()
            if not data:
                return []
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


class MinimalMsgspecJSONStore(MinimalFileStore):
    @property
    def name(self) -> str:
        return f"json://{self.path}"

    def update_file(self):
        """
        Updates the json file when a write-like operation is performed.
        """
        import msgspec

        with zopen(self.path, "wb") as f:
            for d in self.data:
                d.pop("_id", None)
            bytesdata = msgspec.json.encode(
                self.data,
            )
            f.write(bytesdata)

    def read_file(self) -> list:
        """
        Helper method to read the contents of a JSON file and generate
        a list of docs.
        """
        import msgspec

        with zopen(self.path, "rb") as f:
            data = f.read()
            if not data:
                return []
            objects = msgspec.json.decode(data)
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
    @property
    def name(self) -> str:
        return f"msgpack://{self.path}"

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
