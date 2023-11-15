from __future__ import annotations

import datetime
import glob
import os
import traceback
from pathlib import Path

from jobflow import JobStore, initialize_logger
from jobflow.core.job import Job
from monty.os import cd
from monty.serialization import dumpfn, loadfn
from monty.shutil import decompress_file

from jobflow_remote.jobs.data import IN_FILENAME, OUT_FILENAME
from jobflow_remote.remote.data import (
    default_orjson_serializer,
    get_remote_store_filenames,
)


def run_remote_job(run_dir: str | Path = "."):
    """Run the job"""

    start_time = datetime.datetime.utcnow()
    with cd(run_dir):
        error = None
        try:
            dumpfn({"start_time": start_time}, OUT_FILENAME)
            in_data = loadfn(IN_FILENAME)

            job: Job = in_data["job"]
            store = in_data["store"]

            # needs to be set here again since it does not get properly serialized.
            # it is possible to serialize the default function before serializing, but
            # avoided that to avoid that any refactoring of the
            # default_orjson_serializer breaks the deserialization of old Fireworks
            store.docs_store.serialization_default = default_orjson_serializer
            for additional_store in store.additional_stores.values():
                additional_store.serialization_default = default_orjson_serializer

            store.connect()

            initialize_logger()
            try:
                response = job.run(store=store)
            finally:
                # some jobs may have compressed the FW files while being executed,
                # try to decompress them if that is the case.
                decompress_files(store)

            # The output of the response has already been stored in the store.
            response.output = None
            output = {
                "response": response,
                "error": error,
                "start_time": start_time,
                "end_time": datetime.datetime.utcnow(),
            }
            dumpfn(output, OUT_FILENAME)
        except Exception:
            # replicate the dump to catch potential errors in
            # serializing/dumping the response.
            error = traceback.format_exc()
            output = {
                "response": None,
                "error": error,
                "start_time": start_time,
                "end_time": datetime.datetime.utcnow(),
            }
            dumpfn(output, OUT_FILENAME)


def decompress_files(store: JobStore):
    file_names = [OUT_FILENAME]
    file_names.extend(get_remote_store_filenames(store))

    for fn in file_names:
        # If the file is already present do not decompress it, even if
        # a compressed version is present.
        if os.path.isfile(fn):
            continue
        for f in glob.glob(fn + ".*"):
            decompress_file(f)
