from __future__ import annotations

import io
import logging
import os
from pathlib import Path
from typing import Any

import orjson
from jobflow.core.store import JobStore
from maggma.stores.mongolike import JSONStore
from monty.json import jsanitize

from jobflow_remote.utils.data import uuid_to_path


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

    docs_store = JSONStore(
        os.path.join(launch_dir, "remote_job_data.json"),
        read_only=False,
        serialization_default=serialization_default,
    )
    additional_stores = {}
    for k in store.additional_stores.keys():
        additional_stores[k] = JSONStore(
            os.path.join(launch_dir, f"additional_store_{k}.json"),
            read_only=False,
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
    filenames = ["remote_job_data.json"]
    for k in store.additional_stores.keys():
        filenames.append(f"additional_store_{k}.json")

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
    return job_dict
