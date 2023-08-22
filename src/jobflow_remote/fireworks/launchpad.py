from __future__ import annotations

import datetime
import logging
import traceback
from dataclasses import asdict, dataclass

from fireworks import Firework, FWAction, Launch, LaunchPad, Workflow
from fireworks.core.launchpad import WFLock, get_action_from_gridfs
from fireworks.utilities.fw_serializers import reconstitute_dates, recursive_dict
from maggma.core import Store
from maggma.stores import MongoStore
from pymongo import ASCENDING, DESCENDING
from qtoolkit.core.data_objects import QState

from jobflow_remote.jobs.state import RemoteState
from jobflow_remote.remote.data import update_store
from jobflow_remote.utils.data import check_dict_keywords
from jobflow_remote.utils.db import MongoLock

logger = logging.getLogger(__name__)


FW_UUID_PATH = "spec._tasks.job.uuid"
FW_INDEX_PATH = "spec._tasks.job.index"
REMOTE_DOC_PATH = "spec.remote"
REMOTE_LOCK_PATH = f"{REMOTE_DOC_PATH}.{MongoLock.LOCK_KEY}"
REMOTE_LOCK_TIME_PATH = f"{REMOTE_DOC_PATH}.{MongoLock.LOCK_TIME_KEY}"


def get_remote_doc(doc: dict) -> dict:
    for k in REMOTE_DOC_PATH.split("."):
        doc = doc.get(k, {})
    return doc


def get_job_doc(doc: dict) -> dict:
    return doc["spec"]["_tasks"][0]["job"]


@dataclass
class RemoteRun:
    launch_id: int
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
    start_time: datetime.datetime | None = None
    end_time: datetime.datetime | None = None

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
        prev_state = d["previous_state"]
        if prev_state is not None:
            prev_state = RemoteState(prev_state)
        qstate = d["queue_state"]
        if qstate is not None:
            qstate = QState(qstate)
        d["state"] = RemoteState(d["state"])
        d["previous_state"] = prev_state
        d["queue_state"] = qstate
        d["lock_id"] = d.pop(MongoLock.LOCK_KEY, None)
        d["lock_time"] = d.pop(MongoLock.LOCK_TIME_KEY, None)
        return cls(**d)

    @property
    def is_locked(self) -> bool:
        return self.lock_id is not None


class RemoteLaunchPad:
    def __init__(self, store: Store):
        if not isinstance(store, MongoStore):
            raise ValueError(
                f"The store should be an instance of a maggma MongoStore. Got {store.__class__} instead"
            )
        self.store = store
        self.store.connect()
        self.lpad = LaunchPad(strm_lvl="CRITICAL")
        self.lpad.db = store._coll.database
        self.lpad.fireworks = self.db.fireworks
        self.lpad.launches = self.db.launches
        self.lpad.offline_runs = self.db.offline_runs
        self.lpad.fw_id_assigner = self.db.fw_id_assigner
        self.lpad.workflows = self.db.workflows
        self.lpad.gridfs_fallback = None

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
        self.fireworks.create_index(FW_UUID_PATH, background=True)
        self.fireworks.create_index(
            [(FW_UUID_PATH, ASCENDING), (FW_INDEX_PATH, DESCENDING)],
            unique=True,
            background=True,
        )

    def forget_remote(self, fwid):
        """
        Delete the remote run  document for the given launch or firework id.

        Args:
            launchid_or_fwid (int): launch od or firework id
            launch_mode (bool): if True then launch id is given.
        """
        q = {"fw_id": fwid}

        self.db.fireworks.update_one(q, {"$unset": {"spec._remote": ""}})

    def add_remote_run(self, launch_id, fw):
        """
        Add the launch and firework to the offline_run collection.

        Args:
            launch_id (int): launch id
        """
        task = fw.tasks[0]
        task.get("job")
        remote_run = RemoteRun(launch_id)

        self.db.fireworks.update_one(
            {"fw_id": fw.fw_id}, {"$set": {REMOTE_DOC_PATH: remote_run.as_db_dict()}}
        )

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

                # Fixed with respect to fireworks.
                # Otherwise the created_on for RUNNING state is wrong
                if not already_running:
                    m_launch.state = "RUNNING"  # this should also add a history item
                    for s in m_launch.state_history:
                        if s["state"] == "RUNNING":
                            s["created_on"] = reconstitute_dates(
                                remote_status["started_on"]
                            )

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
                            "updated_on": datetime.datetime.utcnow().isoformat(),
                        }
                    },
                )
                if f:
                    self.lpad._refresh_wf(fw_id)

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

                completed = True
        return m_launch, completed

    def add_wf(self, wf):
        return self.lpad.add_wf(wf)

    def get_fw_dict(
        self,
        fw_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ):
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
        query, sort = self.generate_id_query(fw_id, job_id, job_index)
        fw_dict = self.fireworks.find_one(query, sort=sort)
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
    def generate_id_query(
        fw_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ) -> tuple[dict, list | None]:
        query: dict = {}
        sort: list | None = None

        if (job_id is None) == (fw_id is None):
            raise ValueError(
                "One and only one among job_id and db_id should be defined"
            )

        if fw_id:
            query["fw_id"] = fw_id
        if job_id:
            query[FW_UUID_PATH] = job_id
            if job_index is None:
                sort = [[FW_INDEX_PATH, DESCENDING]]
            else:
                query[FW_INDEX_PATH] = job_index
        if not query:
            raise ValueError("At least one among fw_id and job_id should be specified")
        return query, sort

    def _check_ids(
        self,
        fw_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ):
        if (job_id is None) == (fw_id is None):
            raise ValueError(
                "One and only one among fw_id and job_id should be defined"
            )
        if job_id:
            fw_id = self.get_fw_id_from_job_id(job_id, job_index)
        return fw_id, job_id

    def get_fw(
        self,
        fw_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ):
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
        return Firework.from_dict(self.get_fw_dict(fw_id, job_id, job_index))

    def get_fw_id_from_job_id(self, job_id: str, job_index: int | None = None):
        query, sort = self.generate_id_query(job_id=job_id, job_index=job_index)
        fw_dict = self.fireworks.find_one(query, projection=["fw_id"], sort=sort)
        if not fw_dict:
            raise ValueError(f"No Firework exists with id: {job_id}")

        return fw_dict["fw_id"]

    def rerun_fw(
        self,
        fw_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
        recover_launch: int | str | None = None,
        recover_mode: str | None = None,
    ):
        """
        Rerun the firework corresponding to the given id.

        Args:
            fw_id (int): firework id
            recover_launch ('last' or int): launch_id for last recovery, if set to
                'last' (default), recovery will find the last available launch.
                If it is an int, will recover that specific launch
            recover_mode ('prev_dir' or 'cp'): flag to indicate whether to copy
                or run recovery fw in previous directory

        Returns:
            [int]: list of firework ids that were rerun
        """
        query, sort = self.generate_id_query(
            fw_id=fw_id, job_id=job_id, job_index=job_index
        )

        m_fw = self.fireworks.find_one(
            query, projection={"state": 1, "fw_id": 1}, sort=sort
        )

        if not m_fw:
            raise ValueError(f"FW with id: {fw_id or job_id} not found!")
        fw_id = m_fw["fw_id"]

        reruns = []

        # Launch recovery
        if recover_launch is not None:
            recovery = self.lpad.get_recovery(fw_id, recover_launch)
            recovery.update({"_mode": recover_mode})
            set_spec = recursive_dict({"$set": {"spec._recovery": recovery}})
            if recover_mode == "prev_dir":
                prev_dir = self.lpad.get_launch_by_id(
                    recovery.get("_launch_id")
                ).launch_dir
                set_spec["$set"]["spec._launch_dir"] = prev_dir
            self.fireworks.find_one_and_update({"fw_id": fw_id}, set_spec)

        # If no launch recovery specified, unset the firework recovery spec
        else:
            set_spec = {"$unset": {"spec._recovery": ""}}
            self.fireworks.find_one_and_update({"fw_id": fw_id}, set_spec)

        # rerun this FW
        if m_fw["state"] in ["ARCHIVED", "DEFUSED"]:
            self.lpad.m_logger.info(
                f"Cannot rerun fw_id: {fw_id}: it is {m_fw['state']}."
            )
        elif m_fw["state"] == "WAITING" and not recover_launch:
            self.lpad.m_logger.debug(
                f"Skipping rerun fw_id: {fw_id}: it is already WAITING."
            )
        else:
            with WFLock(self.lpad, fw_id):
                wf = self.lpad.get_wf_by_fw_id_lzyfw(fw_id)
                updated_ids = wf.rerun_fw(fw_id)
                # before updating the fireworks in the database deal with the
                # remote part of the document in the fireworks. Copy the content to
                # archived ones and remove the "remote" from the FW.
                remote_docs = []
                for fw in wf.fws:
                    if fw.fw_id in updated_ids:
                        remote_doc = fw.spec.pop("remote")
                        if remote_doc:
                            remote_docs.append(remote_doc)

                if remote_docs:
                    self.archived_remote_runs.insert_many(remote_docs)

                # now update the fw and wf in the db
                self.lpad._update_wf(wf, updated_ids)
                reruns.append(fw_id)

        return reruns

    def set_remote_values(
        self,
        values: dict,
        fw_id: int | None,
        job_id: str | None = None,
        job_index: int | None = None,
        break_lock: bool = False,
    ) -> bool:
        lock_filter, sort = self.generate_id_query(fw_id, job_id, job_index)
        with MongoLock(
            collection=self.fireworks,
            filter=lock_filter,
            break_lock=break_lock,
            lock_subdoc=REMOTE_DOC_PATH,
            sort=sort,
        ) as lock:
            if lock.locked_document:
                values = {f"{REMOTE_DOC_PATH}.{k}": v for k, v in values.items()}
                values["updated_on"] = datetime.datetime.utcnow().isoformat()
                lock.update_on_release = {"$set": values}
                return True

        return False

    def remove_lock(self, query: dict | None = None) -> int:
        result = self.fireworks.update_many(
            filter=query,
            update={"$unset": {REMOTE_LOCK_PATH: "", REMOTE_LOCK_TIME_PATH: ""}},
        )
        return result.modified_count

    def is_locked(
        self,
        fw_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ) -> bool:
        query, sort = self.generate_id_query(fw_id, job_id, job_index)
        result = self.fireworks.find_one(
            query, projection=[REMOTE_LOCK_PATH], sort=sort
        )
        if not result:
            raise ValueError("No job matching id")
        return REMOTE_LOCK_PATH in result

    def reset_failed_state(
        self,
        fw_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ) -> bool:
        lock_filter, sort = self.generate_id_query(fw_id, job_id, job_index)
        with MongoLock(
            collection=self.fireworks,
            filter=lock_filter,
            lock_subdoc=REMOTE_DOC_PATH,
            sort=sort,
        ) as lock:
            doc = lock.locked_document
            remote = get_remote_doc(doc)
            if remote:
                state = remote["state"]
                if state != RemoteState.FAILED.value:
                    raise ValueError("Job is not in a FAILED state")
                previous_state = remote["previous_state"]
                try:
                    RemoteState(previous_state)
                except ValueError:
                    raise ValueError(
                        f"The registered previous state: {previous_state} is not a valid state"
                    )
                set_dict = {
                    "state": previous_state,
                    "step_attempts": 0,
                    "retry_time_limit": None,
                    "previous_state": None,
                    "queue_state": None,
                    "error": None,
                }
                for k, v in list(set_dict.items()):
                    set_dict[f"{REMOTE_DOC_PATH}.{k}"] = v
                    set_dict.pop(k)
                set_dict["updated_on"] = datetime.datetime.utcnow().isoformat()

                lock.update_on_release = {"$set": set_dict}
                return True

        return False

    def delete_wf(self, fw_id: int | None = None, job_id: str | None = None):
        """
        Delete the workflow containing firework with the given id.

        """
        # index is not needed here, since all the jobs with one job_id will
        # belong to the same Workflow
        fw_id, job_id = self._check_ids(fw_id, job_id)

        links_dict = self.workflows.find_one({"nodes": fw_id})
        if not links_dict:
            raise ValueError(
                f"No Flow matching the criteria db_id: {fw_id} job_id: {job_id}"
            )
        fw_ids = links_dict["nodes"]
        self.lpad.delete_fws(fw_ids, delete_launch_dirs=False)
        self.archived_remote_runs.delete_many({"fw_id": {"$in": fw_ids}})
        self.workflows.delete_one({"nodes": fw_id})

    def get_remote_run(
        self,
        fw_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ) -> RemoteRun:

        query, sort = self.generate_id_query(fw_id, job_id, job_index)

        fw = self.fireworks.find_one(query)
        if not fw:
            msg = f"No Job exists with fw id: {fw_id} or job_id {job_id}"
            if job_index is not None:
                msg += f" and job index {job_index}"
            raise ValueError(msg)

        remote_dict = get_remote_doc(fw)
        if not remote_dict:
            msg = f"No Remote run exists with fw id: {fw_id} or job_id {job_id}"
            if job_index is not None:
                msg += f" and job index {job_index}"
        raise ValueError(msg)

        return RemoteRun.from_db_dict(remote_dict)

    def get_fws(
        self, query: dict | None = None, sort: list[tuple] | None = None, limit: int = 0
    ) -> list[Firework]:
        result = self.fireworks.find(query, sort=sort, limit=limit)

        fws = []
        for doc in result:
            fws.append(Firework.from_dict(doc))
        return fws

    def get_fw_remote_run(
        self,
        query: dict | None = None,
        projection: dict | None = None,
        sort: list | None = None,
        limit: int = 0,
    ) -> list[tuple[Firework, RemoteRun | None]]:
        fws = self.fireworks.find(query, projection=projection, sort=sort, limit=limit)

        data = []
        for fw_dict in fws:
            r = get_remote_doc(fw_dict)
            if r:
                remote_run = RemoteRun.from_db_dict(r)
            else:
                remote_run = None

            # remove the launches as they will require additional queries to the db
            fw_dict.pop("launches")
            fw_dict.pop("archived_launches")

            fw = Firework.from_dict(fw_dict)
            data.append((fw, remote_run))

        return data

    def get_fw_ids(
        self, query: dict | None = None, sort: dict | None = None, limit: int = 0
    ) -> list[int]:
        result = self.fireworks.find(
            filter=query, sort=sort, limit=limit, projection={"fw_id": 1}
        )

        fw_ids = []
        for doc in result:
            fw_ids.append(doc["fw_id"])

        return fw_ids

    def get_fw_remote_run_from_id(
        self,
        fw_id: int | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ) -> tuple[Firework, RemoteRun] | None:
        query, sort = self.generate_id_query(fw_id, job_id, job_index)
        results = self.get_fw_remote_run(query=query, sort=sort)
        if not results:
            return None
        return results[0]

    def get_wf_fw_data(
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
            }
        ]

        if query:
            pipeline.append({"$match": query})

        if projection:
            pipeline.append({"$project": projection})

        if sort:
            pipeline.append({"$sort": {k: v for (k, v) in sort}})

        if limit:
            pipeline.append({"$limit": limit})

        return list(self.workflows.aggregate(pipeline))

    def get_wf_fw_remote_run(
        self, query: dict | None = None, sort: dict | None = None, limit: int = 0
    ) -> list[tuple[Workflow, dict[int, RemoteRun]]]:
        raw_data = self.get_wf_fw_data(query=query, sort=sort, limit=limit)

        data = []
        for d in raw_data:
            fws = d["fws"]
            remotes_dict = {}
            for fw_dict in fws:
                r = get_remote_doc(fw_dict)
                if r:
                    remotes_dict[fw_dict["fw_id"]] = RemoteRun.from_db_dict(r)

            wf = Workflow.from_dict(d)
            data.append((wf, remotes_dict))

        return data

    def get_wf_ids(
        self, query: dict | None = None, sort: dict | None = None, limit: int = 0
    ) -> list[int]:
        full_required = check_dict_keywords(query, ["fws."])

        if full_required:
            result = self.get_wf_fw_data(
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
        pipeline: list[dict] = [
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
