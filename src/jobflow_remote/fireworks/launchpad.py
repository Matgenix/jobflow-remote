from __future__ import annotations

import datetime
import logging
import traceback
from dataclasses import asdict, dataclass

from fireworks import Firework, FWAction, Launch, LaunchPad, Workflow
from fireworks.core.launchpad import get_action_from_gridfs
from fireworks.utilities.fw_serializers import reconstitute_dates
from pymongo import ASCENDING
from qtoolkit.core.data_objects import QState

from jobflow_remote.jobs.state import RemoteState
from jobflow_remote.remote.data import update_store
from jobflow_remote.utils.data import check_dict_keywords
from jobflow_remote.utils.db import MongoLock

logger = logging.getLogger(__name__)


fw_uuid = "spec._tasks.job.uuid"


@dataclass
class RemoteRun:
    fw_id: int
    launch_id: int
    name: str
    job_id: str
    machine_id: str
    created_on: str = datetime.datetime.utcnow().isoformat()
    updated_on: str = datetime.datetime.utcnow().isoformat()
    state: RemoteState = RemoteState.CHECKED_OUT
    step_attempts: int = 0
    retry_time_limit: datetime.datetime | None = None
    previous_state: RemoteState | None = None
    queue_state: QState | None = None
    error: str | None = None
    lock_id: str | None = None
    lock_time: datetime.datetime | None = None
    process_id: str | None = None
    run_dir: str | None = None

    def as_db_dict(self):
        d = asdict(self)
        d["state"] = d["state"].value
        d["previous_state"] = d["previous_state"].value if self.previous_state else None
        d["queue_state"] = d["queue_state"].value if self.queue_state else None
        d.pop("lock_id")
        d.pop("lock_time")
        if self.lock_id is not None:
            d[MongoLock.LOCK_KEY] = self.lock_id
        if self.lock_time is not None:
            d[MongoLock.LOCK_TIME_KEY] = self.lock_time
        return d

    @classmethod
    def from_db_dict(cls, d: dict) -> RemoteRun:
        d["state"] = RemoteState(d["state"])
        d["previous_state"] = RemoteState(d["previous_state"])
        d["queue_state"] = QState(d["queue_state"])
        d.pop("_id", None)
        d["lock_id"] = d.pop(MongoLock.LOCK_KEY, None)
        d["lock_time"] = d.pop(MongoLock.LOCK_TIME_KEY, None)
        return cls(**d)

    @property
    def is_locked(self) -> bool:
        return self.lock_id is not None


class RemoteLaunchPad:
    def __init__(self, **kwargs):
        self.lpad = LaunchPad(**kwargs)
        self.remote_runs = self.db.remote_runs
        self.archived_remote_runs = self.db.archived_remote_runs

    @property
    def db(self):
        return self.lpad.db

    @property
    def fireworks(self):
        return self.lpad.fireworks

    @property
    def workflows(self):
        return self.lpad.workflows

    @property
    def launches(self):
        return self.lpad.launches

    def reset(self, password, require_password=True, max_reset_wo_password=25):
        self.lpad.reset(password, require_password, max_reset_wo_password)
        self.remote_runs.delete_many({})
        self.fireworks.create_index(fw_uuid, unique=True, background=True)
        self.remote_runs.create_index("job_id", unique=True, background=True)
        self.remote_runs.create_index("launch_id", unique=True, background=True)
        self.remote_runs.create_index("fw_id", unique=True, background=True)

    def forget_remote(self, launchid_or_fwid, launch_mode=True):
        """
        Delete the remote run  document for the given launch or firework id.

        Args:
            launchid_or_fwid (int): launch od or firework id
            launch_mode (bool): if True then launch id is given.
        """
        q = (
            {"launch_id": launchid_or_fwid}
            if launch_mode
            else {"fw_id": launchid_or_fwid}
        )
        self.db.remote_runs.delete_many(q)

    def add_remote_run(self, launch_id, fw):
        """
        Add the launch and firework to the offline_run collection.

        Args:
            launch_id (int): launch id
        """
        task = fw.tasks[0]
        job = task.get("job")
        remote_run = RemoteRun(
            fw_id=fw.fw_id,
            launch_id=launch_id,
            name=fw.name,
            job_id=job.uuid,
            machine_id=task.get("machine"),
        )

        self.db.remote_runs.insert_one(remote_run.as_db_dict())

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

    def add_wf(self, wf):
        return self.lpad.add_wf(wf)

    def get_fw_dict(self, fw_id: int | None = None, job_id: str | None = None):
        """
        Given a fw id or a job id, return firework dict.

        Parameters
        ----------
        fw_id: int
            The fw_id of the Firework
        job_id: str
            The job_id of the Firework to retrieve

        Returns
        -------
        dict
            The dictionary defining the Firework
        """
        query = self._generate_id_query(fw_id, job_id)
        fw_dict = self.fireworks.find_one(query)
        if not fw_dict:
            raise ValueError(
                f"No Firework exists with fw id: {fw_id} or job_id {job_id}"
            )
        # recreate launches from the launch collection
        launches = list(
            self.launches.find(
                {"launch_id": {"$in": fw_dict["launches"]}},
                sort=[("launch_id", ASCENDING)],
            )
        )
        for launch in launches:
            launch["action"] = get_action_from_gridfs(
                launch.get("action"), self.lpad.gridfs_fallback
            )
        fw_dict["launches"] = launches
        launches = list(
            self.launches.find(
                {"launch_id": {"$in": fw_dict["archived_launches"]}},
                sort=[("launch_id", ASCENDING)],
            )
        )
        for launch in launches:
            launch["action"] = get_action_from_gridfs(
                launch.get("action"), self.lpad.gridfs_fallback
            )
        fw_dict["archived_launches"] = launches
        return fw_dict

    @staticmethod
    def _generate_id_query(fw_id: int | None = None, job_id: str | None = None) -> dict:
        query: dict = {}
        if fw_id:
            query["fw_id"] = fw_id
        if job_id:
            query[fw_uuid] = job_id
        if not query:
            raise ValueError("At least one among fw_id and job_id should be specified")
        return query

    def _check_ids(self, fw_id: int | None = None, job_id: str | None = None):
        if job_id is None and fw_id is None:
            raise ValueError("At least one among fw_id and job_id should be defined")
        if job_id:
            fw_id = self.get_fw_id_from_job_id(job_id)
        return fw_id, job_id

    def get_fw(self, fw_id: int | None = None, job_id: str | None = None):
        """
        Given a fw id or a job id, return the Firework object.

        Parameters
        ----------
        fw_id: int
            The fw_id of the Firework
        job_id: str
            The job_id of the Firework to retrieve

        Returns
        -------
        Firework
            The retrieved Firework
        """
        return Firework.from_dict(self.get_fw_dict(fw_id, job_id))

    def get_fw_id_from_job_id(self, job_id: str):
        fw_dict = self.fireworks.find_one({fw_uuid: job_id}, projection=["fw_id"])
        if not fw_dict:
            raise ValueError(f"No Firework exists with id: {job_id}")

        return fw_dict["fw_id"]

    def rerun_fw(
        self,
        fw_id: int | None = None,
        job_id: str | None = None,
        rerun_duplicates: bool = True,
        recover_launch: int | str | None = None,
        recover_mode: str | None = None,
    ):
        fw_id, job_id = self._check_ids(fw_id, job_id)
        rerun_fw_ids = self.lpad.rerun_fw(
            fw_id, rerun_duplicates, recover_launch, recover_mode
        )

        to_archive = self.remote_runs.find({"fw_id": {"$in": rerun_fw_ids}})
        for doc in to_archive:
            doc.pop("_id", None)
            self.archived_remote_runs.insert(doc)

        self.remote_runs.delete_many({"fw_id": {"$in": rerun_fw_ids}})

    def set_remote_state(
        self,
        state: RemoteState,
        fw_id: int | None,
        job_id: str | None = None,
        break_lock: bool = False,
    ):
        lock_filter = self._generate_id_query(fw_id, job_id)
        with MongoLock(
            collection=self.remote_runs, filter=lock_filter, break_lock=break_lock
        ) as lock:
            if lock.locked_document:
                lock.update_on_release = {
                    "$set": {
                        "state": state.value,
                        "updated_on": datetime.datetime.utcnow().isoformat(),
                        "completed": False,
                        "step_attempts": 0,
                        "retry_time_limit": None,
                        "previous_state": None,
                        "queue_state": None,
                        "error": None,
                    }
                }
                return True

        return False

    def remove_lock(self, fw_id: int | None = None, job_id: str | None = None):
        query = self._generate_id_query(fw_id, job_id)
        result = self.remote_runs.find_one_and_update(
            query,
            {"$unset": {MongoLock.LOCK_KEY: "", MongoLock.LOCK_TIME_KEY: ""}},
            projection=["fw_id"],
        )
        if not result:
            raise ValueError("No job matching id")

    def is_locked(self, fw_id: int | None = None, job_id: str | None = None):
        query = self._generate_id_query(fw_id, job_id)
        result = self.remote_runs.find_one(query, projection=[MongoLock.LOCK_KEY])
        if not result:
            raise ValueError("No job matching id")
        return MongoLock.LOCK_KEY in result

    def reset_failed_state(self, fw_id: int | None = None, job_id: str | None = None):
        lock_filter = self._generate_id_query(fw_id, job_id)
        with MongoLock(collection=self.remote_runs, filter=lock_filter) as lock:
            doc = lock.locked_document
            if doc:
                state = doc["state"]
                if state != RemoteState.FAILED.value:
                    raise ValueError("Job is not in a FAILED state")
                previous_state = doc["previous_state"]
                try:
                    RemoteState(previous_state)
                except ValueError:
                    raise ValueError(
                        f"The registered previous state: {previous_state} is not a valid state"
                    )
                lock.update_on_release = {
                    "$set": {
                        "state": previous_state,
                        "updated_on": datetime.datetime.utcnow().isoformat(),
                        "completed": False,
                        "step_attempts": 0,
                        "retry_time_limit": None,
                        "previous_state": None,
                        "queue_state": None,
                        "error": None,
                    }
                }
                return True

        return False

    def delete_wf(self, fw_id: int | None = None, job_id: str | None = None):
        """
        Delete the workflow containing firework with the given id.

        """
        fw_id, job_id = self._check_ids(fw_id, job_id)

        links_dict = self.workflows.find_one({"nodes": fw_id})
        fw_ids = links_dict["nodes"]
        self.lpad.delete_fws(fw_ids, delete_launch_dirs=False)
        self.remote_runs.delete_many({"fw_id": {"$in": fw_ids}})
        self.archived_remote_runs.delete_many({"fw_id": {"$in": fw_ids}})
        self.workflows.delete_one({"nodes": fw_id})

    def get_remote_run(
        self, fw_id: int | None = None, job_id: str | None = None
    ) -> RemoteRun:
        query = self._generate_id_query(fw_id, job_id)
        remote_run_dict = self.remote_runs.find_one(query)
        if not remote_run_dict:
            raise ValueError(
                f"No Firework exists with fw id: {fw_id} or job_id {job_id}"
            )

        return RemoteRun.from_db_dict(remote_run_dict)

    def get_fws(
        self, query: dict | None = None, sort: list[tuple] | None = None, limit: int = 0
    ) -> list[Firework]:
        result = self.fireworks.find(query, sort=sort, limit=limit)

        fws = []
        for doc in result:
            fws.append(Firework.from_dict(doc))
        return fws

    def get_fw_remote_run_data(
        self,
        query: dict | None = None,
        projection: dict | None = None,
        sort: dict | None = None,
        limit: int = 0,
    ) -> list[dict]:

        pipeline: list[dict] = [
            {
                "$lookup": {
                    "from": "remote_runs",
                    "localField": "fw_id",
                    "foreignField": "fw_id",
                    "as": "remote",
                }
            }
        ]

        if query:
            pipeline.append({"$match": query})

        if projection:
            pipeline.append({"$project": projection})

        if sort:
            pipeline.append({"$sort": sort})

        if limit:
            pipeline.append({"$limit": limit})

        return list(self.fireworks.aggregate(pipeline))

    def get_fw_remote_run(
        self,
        query: dict | None = None,
        projection: dict | None = None,
        sort: dict | None = None,
        limit: int = 0,
    ) -> list[tuple[Firework, RemoteRun | None]]:
        raw_data = self.get_fw_remote_run_data(
            query=query, projection=projection, sort=sort, limit=limit
        )

        data = []
        for d in raw_data:
            r = d.pop("remote", None)
            if r:
                if len(r) > 1:
                    raise RuntimeError(
                        f"error retrieving the remote_run document. {len(r)} found. Expected 1."
                    )
                remote_run = RemoteRun.from_db_dict(r[0])
            else:
                remote_run = None

            fw = Firework.from_dict(d)
            data.append((fw, remote_run))

        return data

    def get_fw_ids(
        self, query: dict | None = None, sort: dict | None = None, limit: int = 0
    ) -> list[int]:
        remote_required = check_dict_keywords(query, ["remote."])
        if remote_required:
            result = self.get_fw_remote_run_data(
                query=query, sort=sort, limit=limit, projection={"fw_id": 1}
            )
        else:
            result = self.fireworks.find(
                query, sort=sort, limit=limit, projection=["fw_id"]
            )

        fw_ids = []
        for doc in result:
            fw_ids.append(doc["fw_id"])

        return fw_ids

    def get_fw_remote_run_from_id(
        self, fw_id: int | None = None, job_id: str | None = None
    ) -> tuple[Firework, RemoteRun] | None:
        if fw_id is None and job_id is None:
            raise ValueError("at least one among fw_id and job_id should be defined")
        query: dict = {}
        if fw_id:
            query["fw_id"] = fw_id
        if job_id:
            query[fw_uuid] = job_id
        results = self.get_fw_remote_run(query=query)
        if not results:
            return None
        return results[0]

    def get_wf_fw_remote_run_data(
        self,
        query: dict | None = None,
        projection: dict | None = None,
        sort: dict | None = None,
        limit: int = 0,
    ) -> list[dict]:

        pipeline: list[dict] = [
            {
                "$lookup": {
                    "from": "fireworks",
                    "localField": "nodes",
                    "foreignField": "fw_id",
                    "as": "fws",
                }
            },
            {
                "$lookup": {
                    "from": "remote_runs",
                    "localField": "nodes",
                    "foreignField": "fw_id",
                    "as": "remote",
                }
            },
        ]

        if query:
            pipeline.append({"$match": query})

        if projection:
            pipeline.append({"$project": projection})

        if sort:
            pipeline.append({"$sort": sort})

        if limit:
            pipeline.append({"$limit": limit})

        return list(self.workflows.aggregate(pipeline))

    def get_wf_fw_remote_run(
        self, query: dict | None = None, sort: dict | None = None, limit: int = 0
    ) -> list[tuple[Workflow, dict[int, RemoteRun]]]:
        raw_data = self.get_wf_fw_remote_run_data(query=query, sort=sort, limit=limit)

        data = []
        for d in raw_data:
            remotes = d.pop("remote", None)

            remotes_dict = {}
            for r in remotes:
                remotes_dict[r["fw_id"]] = RemoteRun.from_db_dict(r)

            wf = Workflow.from_dict(d)
            data.append((wf, remotes_dict))

        return data

    def get_wf_ids(
        self, query: dict | None = None, sort: dict | None = None, limit: int = 0
    ) -> list[int]:
        full_required = check_dict_keywords(query, ["remote.", "fws."])

        if full_required:
            result = self.get_wf_fw_remote_run_data(
                query=query, sort=sort, limit=limit, projection={"fw_id": 1}
            )
        else:
            result = self.lpad.get_wf_ids(query, sort=sort, limit=limit)

        fw_ids = []
        for doc in result:
            fw_ids.append(doc["fw_id"])

        return fw_ids

    def get_fw_launch_remote_run_data(
        self,
        query: dict | None = None,
        projection: dict | None = None,
        sort: dict | None = None,
        limit: int = 0,
    ) -> list[dict]:

        # only take the most recent launch
        pipeline = [
            {
                "$lookup": {
                    "from": "remote_runs",
                    "localField": "fw_id",
                    "foreignField": "fw_id",
                    "as": "remote",
                }
            },
            {
                "$lookup": {
                    "from": "launches",
                    "localField": "fw_id",
                    "foreignField": "fw_id",
                    "as": "launch",
                    "pipeline": [{"$sort": {"time_start": -1}}, {"$limit": 1}],
                }
            },
        ]

        if query:
            pipeline.append({"$match": query})

        if projection:
            pipeline.append({"$project": projection})

        if sort:
            pipeline.append({"$sort": sort})

        if limit:
            pipeline.append({"$limit": limit})

        return list(self.fireworks.aggregate(pipeline))
