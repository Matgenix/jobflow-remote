from __future__ import annotations

import datetime
import glob
import logging
import os
import subprocess
import time
import traceback
from multiprocessing import Manager, Process
from typing import TYPE_CHECKING

from jobflow import JobStore, initialize_logger
from jobflow.core.flow import get_flow
from monty.os import cd
from monty.serialization import dumpfn, loadfn
from monty.shutil import decompress_file

from jobflow_remote.jobs.batch import LocalBatchManager
from jobflow_remote.jobs.data import IN_FILENAME, OUT_FILENAME
from jobflow_remote.remote.data import get_job_path, get_store_file_paths
from jobflow_remote.utils.log import initialize_remote_run_log

if TYPE_CHECKING:
    from pathlib import Path

    from jobflow.core.job import Job

logger = logging.getLogger(__name__)


def run_remote_job(run_dir: str | Path = ".") -> None:
    """Run the job."""
    initialize_remote_run_log()

    start_time = datetime.datetime.utcnow()
    with cd(run_dir):
        error = None
        try:
            dumpfn({"start_time": start_time}, OUT_FILENAME)
            in_data = loadfn(IN_FILENAME)

            job: Job = in_data["job"]
            store = in_data["store"]

            store.connect()

            initialize_logger()
            try:
                response = job.run(store=store)
            finally:
                # some jobs may have compressed the jfremote and store files while being
                # executed, try to decompress them if that is the case and files need to be
                # decompressed.
                decompress_files(store)

            # Close the store explicitly, as minimal stores may require it.
            try:
                store.close()
            except Exception:
                logger.exception("Error while closing the store")

            # The output of the response has already been stored in the store.
            response.output = None

            # Convert to Flow the dynamic responses before dumping the output.
            # This is required so that the response does not need to be
            # deserialized and converted to Flows by the runner.
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
    parallel_jobs: int | None = None,
) -> None:
    parallel_jobs = parallel_jobs or 1

    if parallel_jobs == 1:
        run_single_batch_jobs(
            base_run_dir=base_run_dir,
            files_dir=files_dir,
            process_uuid=process_uuid,
            max_time=max_time,
            max_wait=max_wait,
            max_jobs=max_jobs,
        )
    else:
        with Manager() as manager:
            multiprocess_lock = manager.Lock()
            parallel_ids = manager.dict()
            batch_manager = LocalBatchManager(
                files_dir=files_dir,
                process_id=process_uuid,
                multiprocess_lock=multiprocess_lock,
            )
            processes = [
                Process(
                    target=run_single_batch_jobs,
                    args=(
                        base_run_dir,
                        files_dir,
                        process_uuid,
                        max_time,
                        max_wait,
                        max_jobs,
                        batch_manager,
                        parallel_ids,
                    ),
                )
                for _ in range(parallel_jobs)
            ]
            for p in processes:
                p.start()
                time.sleep(0.5)

            for p in processes:
                p.join()


def run_single_batch_jobs(
    base_run_dir: str | Path,
    files_dir: str | Path,
    process_uuid: str,
    max_time: int | None = None,
    max_wait: int = 60,
    max_jobs: int | None = None,
    batch_manager: LocalBatchManager | None = None,
    parallel_ids: dict | None = None,
) -> None:
    initialize_remote_run_log()

    # TODO the ID should be somehow linked to the queue job
    if not batch_manager:
        batch_manager = LocalBatchManager(files_dir=files_dir, process_id=process_uuid)

    if parallel_ids:
        parallel_ids[os.getpid()] = False

    t0 = time.time()
    wait = 0
    sleep_time = 10
    count = 0
    while True:
        if max_time and max_time < time.time() - t0:
            logger.info("Stopping due to max_time")
            return

        if max_wait and wait > max_wait:
            # if many jobs run in parallel do not shut down here, unless all
            # the other jobs are also stopped
            if parallel_ids:
                for pid, pid_is_running in parallel_ids.items():
                    if pid_is_running:
                        try:
                            os.kill(pid, 0)  # throws OSError if the process is dead
                        except OSError:  # means this process is dead!
                            pid_is_running[pid] = False
                if not any(parallel_ids.values()):
                    logger.info(
                        f"No jobs available for more than {max_wait} seconds and all other jobs are stopped. Stopping."
                    )
                    return
            else:
                logger.info(
                    f"No jobs available for more than {max_wait} seconds. Stopping."
                )
                return

        if max_jobs and count >= max_jobs:
            logger.info(f"Maximum number of jobs reached ({max_jobs}). Stopping.")
            return

        job_str = batch_manager.get_job()
        if not job_str:
            time.sleep(sleep_time)
            wait += sleep_time
        else:
            wait = 0
            count += 1
            job_id, _index = job_str.split("_")
            index: int = int(_index)
            logger.info(f"Starting job with id {job_id} and index {index}")
            job_path = get_job_path(job_id=job_id, index=index, base_path=base_run_dir)
            if parallel_ids:
                parallel_ids[os.getpid()] = True
            try:
                with cd(job_path):
                    result = subprocess.run(
                        ["bash", "submit.sh"],  # noqa: S603, S607
                        check=True,
                        text=True,
                        capture_output=True,
                    )
                    if result.returncode:
                        logger.warning(
                            f"Process for job with id {job_id} and index {index} finished with an error"
                        )
                batch_manager.terminate_job(job_id, index)
            except Exception:
                logger.exception(
                    "Error while running job with id {job_id} and index {index}"
                )
            else:
                logger.info(f"Completed job with id {job_id} and index {index}")

        if parallel_ids:
            parallel_ids[os.getpid()] = False


def decompress_files(store: JobStore) -> None:
    file_names = [OUT_FILENAME]
    file_names.extend(os.path.basename(p) for p in get_store_file_paths(store))

    for fn in file_names:
        # If the file is already present do not decompress it, even if
        # a compressed version is present.
        if os.path.isfile(fn):
            continue
        for f in glob.glob(fn + ".*"):
            decompress_file(f)
