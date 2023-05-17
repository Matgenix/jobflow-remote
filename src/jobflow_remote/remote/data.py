from __future__ import annotations

import logging
import os
from pathlib import Path

from jobflow.core.store import JobStore
from maggma.stores.mongolike import JSONStore

from jobflow_remote.utils.data import uuid_to_path


def get_job_path(job_id: str, base_path: str | Path | None = None) -> Path:
    if base_path:
        base_path = Path(base_path)
    else:
        base_path = Path()

    relative_path = uuid_to_path(job_id)
    return base_path / relative_path


def get_remote_files(fw, launch_id):
    files = {
        # TODO handle binary data?
        "FW.json": fw.to_format(f_format="json"),
        "FW_offline.json": f'{{"launch_id": {launch_id}}}',
    }

    return files


def get_remote_store(store, launch_dir):

    docs_store = JSONStore(
        os.path.join(launch_dir, "remote_job_data.json"), read_only=False
    )
    additional_stores = {}
    for k in store.additional_stores.keys():
        additional_stores[k] = JSONStore(
            os.path.join(launch_dir, f"additional_store_{k}.json"), read_only=False
        )
    remote_store = JobStore(
        docs_store=docs_store,
        additional_stores=additional_stores,
        save=store.save,
        load=store.load,
    )

    remote_store.connect()

    return remote_store


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
