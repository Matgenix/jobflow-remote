from __future__ import annotations

import datetime
import traceback

from fireworks import FWAction, Launch, LaunchPad
from fireworks.utilities.fw_serializers import reconstitute_dates

from jobflow_remote.remote.data import update_store
from jobflow_remote.run.state import RemoteState


class RemoteLaunchPad:
    def __init__(self, **kwargs):
        self.lpad = LaunchPad(**kwargs)
        self.remote_runs = self.db.remote_runs

    @property
    def db(self):
        return self.lpad.db

    def reset(self, password, require_password=True, max_reset_wo_password=25):
        self.lpad.reset(password, require_password, max_reset_wo_password)
        self.remote_runs.delete_many({})

    def forget_remote(self, launchid_or_fwid, launch_mode=True):
        """
        Unmark the offline run for the given launch or firework id.

        Args:
            launchid_or_fwid (int): launch od or firework id
            launch_mode (bool): if True then launch id is given.
        """
        q = (
            {"launch_id": launchid_or_fwid}
            if launch_mode
            else {"fw_id": launchid_or_fwid}
        )
        self.db.remote_runs.update_many(q, {"$set": {"deprecated": True}})

    def add_remote_run(self, launch_id, fw):
        """
        Add the launch and firework to the offline_run collection.

        Args:
            launch_id (int): launch id
            fw_id (id): firework id
            name (str)
        """
        task = fw.tasks[0]
        machine_id = task.get("machine")
        job = task.get("job")
        job_id = job.uuid
        d = {"fw_id": fw.fw_id}
        d["launch_id"] = launch_id
        d["name"] = fw.name
        d["created_on"] = datetime.datetime.utcnow().isoformat()
        d["updated_on"] = datetime.datetime.utcnow().isoformat()
        d["deprecated"] = False
        d["state"] = RemoteState.CHECKED_OUT.value
        d["completed"] = False
        d["job_id"] = job_id
        d["step_attempts"] = 0
        d["retry_time_limit"] = None
        d["failed_state"] = None
        d["queue_state"] = None
        d["machine_id"] = machine_id
        self.db.remote_runs.insert_one(d)

    def get_fw_by_job_id(self, job_id):
        pass

    def get_job_by_uuid(self, job_id):
        self.get_fw_by_job_id(job_id)

    def recover_remote(
        self,
        remote_status,
        launch_id,
        store,
        remote_store,
        save,
        terminated=True,
        ignore_errors=False,
        print_errors=False,
    ):
        """
        Update the launch state using the offline data in FW_offline.json file.

        Args:
            launch_id (int): launch id
            ignore_errors (bool)
            print_errors (bool)

        Returns:
            firework id if the recovering fails otherwise None
        """

        # get the launch directory
        m_launch = self.lpad.get_launch_by_id(launch_id)
        completed = False
        try:
            self.lpad.m_logger.debug(f"RECOVERING fw_id: {m_launch.fw_id}")

            if "started_on" in remote_status:  # started running at some point
                already_running = False
                for s in m_launch.state_history:
                    if s["state"] == "RUNNING":
                        s["created_on"] = reconstitute_dates(
                            remote_status["started_on"]
                        )
                        already_running = True

                if not already_running:
                    m_launch.state = "RUNNING"  # this should also add a history item

                remote_status["checkpoint"] if "checkpoint" in remote_status else None

            status = remote_status.get("state")
            if terminated and status not in ("COMPLETED", "FIZZLED"):
                raise RuntimeError(
                    "The remote job should be terminated, but the Firework did not finish"
                )

            if "fwaction" in remote_status:
                fwaction = FWAction.from_dict(remote_status["fwaction"])
                m_launch.state = remote_status["state"]
                self.lpad.launches.find_one_and_replace(
                    {"launch_id": m_launch.launch_id},
                    m_launch.to_db_dict(),
                    upsert=True,
                )

                m_launch = Launch.from_dict(
                    self.lpad.complete_launch(launch_id, fwaction, m_launch.state)
                )

                for s in m_launch.state_history:
                    if s["state"] == remote_status["state"]:
                        s["created_on"] = reconstitute_dates(
                            remote_status["completed_on"]
                        )
                self.lpad.launches.find_one_and_update(
                    {"launch_id": m_launch.launch_id},
                    {"$set": {"state_history": m_launch.state_history}},
                )

                self.lpad.offline_runs.update_one(
                    {"launch_id": launch_id}, {"$set": {"completed": True}}
                )
                completed = True

            else:
                previous_launch = self.lpad.launches.find_one_and_replace(
                    {"launch_id": m_launch.launch_id},
                    m_launch.to_db_dict(),
                    upsert=True,
                )
                fw_id = previous_launch["fw_id"]
                f = self.lpad.fireworks.find_one_and_update(
                    {"fw_id": fw_id},
                    {
                        "$set": {
                            "state": "RUNNING",
                            "updated_on": datetime.datetime.utcnow(),
                        }
                    },
                )
                if f:
                    self.lpad._refresh_wf(fw_id)

            # update the updated_on
            self.remote_runs.update_one(
                {"launch_id": launch_id},
                {"$set": {"updated_on": datetime.datetime.utcnow().isoformat()}},
            )
            # return None

            if completed:
                update_store(store, remote_store, save)

        except Exception:
            if print_errors:
                self.lpad.m_logger.error(
                    f"failed recovering launch_id {launch_id}.\n{traceback.format_exc()}"
                )
            if not ignore_errors:
                traceback.print_exc()
                m_action = FWAction(
                    stored_data={
                        "_message": "runtime error during task",
                        "_task": None,
                        "_exception": {
                            "_stacktrace": traceback.format_exc(),
                            "_details": None,
                        },
                    },
                    exit=True,
                )
                self.lpad.complete_launch(launch_id, m_action, "FIZZLED")
                self.remote_runs.update_one(
                    {"launch_id": launch_id}, {"$set": {"completed": True}}
                )
                completed = True
        return m_launch.fw_id, completed
