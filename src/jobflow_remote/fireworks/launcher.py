from __future__ import annotations

import logging
from pathlib import Path

from fireworks.core.fworker import FWorker

from jobflow_remote.fireworks.launchpad import RemoteLaunchPad

logger = logging.getLogger(__name__)


def checkout_remote(
    rlpad: RemoteLaunchPad,
    fworker: FWorker | None = None,
    launcher_dir: str | Path = ".",
    strm_lvl: str = "INFO",
    fw_id: int = None,
):
    """
    Submit a single job to the queue.

    Args:
        rlpad
        fworker
        launcher_dir: The directory where to submit the job
        strm_lvl: level at which to stream log messages
        fw_id: specific fw_id to reserve (reservation mode only)
    """
    fworker = fworker if fworker else FWorker()
    # launcher_dir = os.path.abspath(launcher_dir)

    fw, launch_id = None, None

    launch_id = None
    try:

        fw, launch_id = rlpad.lpad.reserve_fw(fworker, launcher_dir, fw_id=fw_id)
        if not fw:
            logger.info("No jobs exist in the LaunchPad for submission to queue!")
            return None, None
        logger.info(f"reserved FW with fw_id: {fw.fw_id}")

        # TODO launcher dir should be set according to remote settings
        # maybe set it later when actually copied?
        # rlpad.change_launch_dir(launch_id, launcher_dir)

        fw.tasks[0].get("job").uuid

        rlpad.add_remote_run(launch_id, fw)

        return fw, launch_id

    except Exception:
        logger.exception("Error writing/submitting queue script!")
        if launch_id is not None:
            try:
                logger.info(
                    f"Un-reserving FW with fw_id, launch_id: {fw.fw_id}, {launch_id}"
                )
                rlpad.lpad.cancel_reservation(launch_id)
                rlpad.forget_remote(launch_id, rlpad)
            except Exception:
                logger.exception(f"Error unreserving FW with fw_id {fw.fw_id}")

        return None, None


def rapidfire_checkout(rlpad, fworker):
    n_checked_out = 0
    while True:
        fw, launch_id = checkout_remote(rlpad, fworker)
        if not fw:
            break

        n_checked_out += 1

    return n_checked_out
