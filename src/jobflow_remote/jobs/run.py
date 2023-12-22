from __future__ import annotations

import datetime
import glob
import logging
import os
import subprocess
import time
import traceback
from pathlib import Path

from jobflow import JobStore, initialize_logger
from jobflow.core.flow import get_flow
from jobflow.core.job import Job
from monty.os import cd
from monty.serialization import dumpfn, loadfn
from monty.shutil import decompress_file

from jobflow_remote.jobs.batch import LocalBatchManager
from jobflow_remote.jobs.data import IN_FILENAME, OUT_FILENAME
from jobflow_remote.remote.data import (
    default_orjson_serializer,
    get_job_path,
    get_remote_store_filenames,
)
from jobflow_remote.utils.log import initialize_remote_run_log

logger = logging.getLogger(__name__)


def run_remote_job(run_dir: str | Path = "."):
    """Run the job"""

    initialize_remote_run_log()

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

            # Convert to Flow the dynamic responses before dumping the output.
            # This is required so that the response does not need to be
            # deserialized and converted by to Flows by the runner.
            if response.addition:
                response.addition = get_flow(response.addition)
            if response.detour:
                response.detour = get_flow(response.detour)
            if response.replace:
                response.replace = get_flow(response.replace)

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


def run_batch_jobs(
    base_run_dir: str | Path,
    files_dir: str | Path,
    process_uuid: str,
    max_time: int | None = None,
    max_wait: int = 60,
    max_jobs: int | None = None,
):
    initialize_remote_run_log()

    # TODO the ID should be somehow linked to the queue job
    bm = LocalBatchManager(files_dir=files_dir, process_id=process_uuid)

    t0 = time.time()
    wait = 0
    sleep_time = 10
    count = 0
    while True:
        if max_time and max_time < time.time() - t0:
            logger.info("Stopping due to max_time")
            return

        if max_wait and wait > max_wait:
            logger.info(
                f"No jobs available for more than {max_wait} seconds. Stopping."
            )
            return

        if max_jobs and count >= max_jobs:
            logger.info(f"Maximum number of jobs reached ({max_jobs}). Stopping.")
            return

        job_str = bm.get_job()
        if not job_str:
            time.sleep(sleep_time)
            wait += sleep_time
        else:
            wait = 0
            count += 1
            job_id, index = job_str.split("_")
            index = int(index)
            logger.info(f"Starting job with id {job_id} and index {index}")
            job_path = get_job_path(job_id=job_id, index=index, base_path=base_run_dir)
            try:
                with cd(job_path):
                    result = subprocess.run(
                        ["bash", "submit.sh"],
                        check=True,
                        text=True,
                        capture_output=True,
                    )
                    if result.returncode:
                        logger.warning(
                            f"Process for job with id {job_id} and index {index} finished with an error"
                        )
                bm.terminate_job(job_id, index)
            except Exception:
                logger.error(
                    "Error while running job with id {job_id} and index {index}",
                    exc_info=True,
                )
            else:
                logger.info(f"Completed job with id {job_id} and index {index}")


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
