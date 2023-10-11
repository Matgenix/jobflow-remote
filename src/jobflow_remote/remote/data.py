from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from jobflow.core.store import JobStore
from maggma.stores.mongolike import JSONStore

from jobflow_remote.utils.data import uuid_to_path


def get_job_path(job_id: str, base_path: str | Path | None = None) -> str:
    if base_path:
        base_path = Path(base_path)
    else:
        base_path = Path()

    relative_path = uuid_to_path(job_id)
    return str(base_path / relative_path)


def get_remote_files(fw, launch_id):
    files = {
        # TODO handle binary data?
        "FW.json": fw.to_format(f_format="json"),
        "FW_offline.json": f'{{"launch_id": {launch_id}}}',
    }

    return files


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


def update_store(store, remote_store, save):
    # TODO is it correct?
    data = list(remote_store.query(load=save))
    if len(data) > 1:
        raise RuntimeError("something wrong with the remote store")

    store.connect()
    try:
        for d in data:
            data = dict(d)
            data.pop("_id")
            store.update(data, key=["uuid", "index"], save=save)
    finally:
        try:
            store.close()
        except Exception:
            logging.error(f"error while closing the store {store}", exc_info=True)
