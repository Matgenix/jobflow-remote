from __future__ import annotations

import contextlib
import functools
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, ClassVar

from packaging.version import Version
from packaging.version import parse as parse_version

import jobflow_remote

if TYPE_CHECKING:
    from collections.abc import Callable

    from pymongo.client_session import ClientSession

    from jobflow_remote.jobs.jobcontroller import JobController

logger = logging.getLogger(__name__)


class UpgradeRequiredError(Exception):
    """
    An error signaling that an upgrade should be performed before performing any further action.
    """


@dataclass
class UpgradeAction:
    """Details of a single upgrade action to be performed"""

    description: str
    collection: str
    action_type: str
    details: dict
    required: bool = False


@dataclass(kw_only=True)
class UpgradeCondition:
    """Generic upgrade condition"""

    description: str
    check_func: (
        Callable[[JobController, UpgradeCondition | None], dict | None] | None
    ) = None
    skippable: bool = False

    def check(self, job_controller: JobController) -> dict | None:
        if self.check_func is None:
            raise NotImplementedError("check_func must be defined")
        result = self.check_func(job_controller, self)
        if result and self.skippable:
            result["message"] += (
                " This condition can be avoided running jf admin upgrade with the `--force` option."
            )
        return result


@dataclass(kw_only=True)
class NoDocumentsIn(UpgradeCondition):
    """Condition that checks that there is no document in a given collection matching the specified query."""

    collection: str | None
    query: dict | None = None
    description: str | None = None

    def __post_init__(self):
        if self.description is None:
            q_str = f" matching {self.query}" if self.query else ""
            self.description = f"There should be no document in the '{self.collection}' collection{q_str}"

        def _check(job_controller: JobController, _=None) -> dict | None:
            coll = getattr(job_controller, self.collection, None)
            if coll is None:
                return None
            count = coll.count_documents(self.query or {})
            if count == 0:
                return None
            return {
                "condition": self,
                "message": f"Found {count} document(s)",
                "count": count,
            }

        self.check_func = _check


class DatabaseUpgrader:
    """
    Object to handle the upgrade of the database between different versions
    """

    _upgrade_registry: ClassVar[dict[Version, Callable]] = {}
    _upgrade_conditions_registry: ClassVar[dict[Version, list]] = {}

    def __init__(self, job_controller: JobController):
        self.job_controller = job_controller
        self.current_version = parse_version(jobflow_remote.__version__)

    @classmethod
    def register_upgrade(cls, version: str, upgrade_conditions: list | None = None):
        """Decorator to register upgrade functions.

        This decorator should be used to register functions that implement the upgrades for each
        version.

        Parameters
        ----------
        version
            The version to register the upgrade function for
        upgrade_conditions
            Conditions required to perform the upgrade (e.g. no jobs in a RUNNING state or no batch process
            submitted or running, ...)
        """

        def decorator(func: Callable):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                logger.info(f"Executing upgrade to version {version}")
                start_time = datetime.now()
                result = func(*args, **kwargs)
                duration = (datetime.now() - start_time).total_seconds()
                logger.info(f"Completed upgrade to {version} in {duration:.2f}s")
                return result

            vv = parse_version(version)
            cls._upgrade_registry[vv] = wrapper
            if upgrade_conditions:
                cls._upgrade_conditions_registry[vv] = upgrade_conditions
            return wrapper

        return decorator

    @property
    def registered_upgrades(self):
        return sorted(self._upgrade_registry.keys())

    def collect_upgrades(
        self, from_version: Version, target_version: Version
    ) -> list[Version]:
        """
        Determines the upgrades that need to be performed.

        from_version
            The version using as a starting point for the list of upgrades.
        target_version
            The final version up to which the upgrades should be listed.
        """
        return [
            v for v in self.registered_upgrades if from_version < v <= target_version
        ]

    def update_db_version(self, version: Version, session: ClientSession | None = None):
        """
        Update the jobflow-remote version information stored in the database.

        Parameters
        ----------
        version
            The version to update the database to
        session
            The client session to use to perform the update
        """
        self.job_controller.auxiliary.update_one(
            {"jobflow_remote_version": {"$exists": True}},
            {"$set": {"jobflow_remote_version": str(version)}},
            upsert=True,
            session=session,
        )

    def check_upgrade_conditions(
        self, versions: list[Version], force: bool = False
    ) -> list[tuple[Version, dict]]:
        failed_conditions = []
        for version in versions:
            upgrade_conditions = self._upgrade_conditions_registry.get(version, [])
            for upgrade_condition in upgrade_conditions:
                if force and upgrade_condition.skippable:
                    continue
                if (check := upgrade_condition.check(self.job_controller)) is not None:
                    failed_conditions.append((version, check))
        return failed_conditions

    def dry_run(
        self,
        from_version: str | None = None,
        target_version: str | None = None,
        force: bool = False,
    ) -> tuple[list[UpgradeAction], list[tuple[Version, dict]]]:
        """Simulate the upgrade process and return all actions that would be performed

        Parameters
        ----------
        from_version
            The version from which to start the upgrade. If ``None``, the current version in the database is used.
        target_version
            The target version of the upgrade. If ``None``, the current version of the package is used.
        force
            Perform the upgrade even if the conditions marked as 'skippable' are not satisfied.

        Returns
        -------
        list
            A list of UpgradeAction objects describing all actions that would be performed
            during the upgrade.
        """
        db_version = (
            parse_version(from_version)
            if from_version
            else self.job_controller.get_current_db_version()
        )
        target_version = (
            parse_version(target_version) if target_version else self.current_version
        )

        if db_version >= target_version:
            return [], []

        versions_needing_upgrade = self.collect_upgrades(db_version, target_version)

        failed_conditions = self.check_upgrade_conditions(
            versions_needing_upgrade, force=force
        )

        all_actions = []
        for version in versions_needing_upgrade:
            upgrade_func = self._upgrade_registry[version]
            actions = upgrade_func(self.job_controller, dry_run=True)
            all_actions.extend(actions)

        # Add the version update action
        all_actions.append(
            UpgradeAction(
                description=f"Update database version number to {target_version}",
                collection="auxiliary",
                action_type="update",
                details={
                    "filter": {"jobflow_remote_version": {"$exists": True}},
                    "update": {"$set": {"jobflow_remote_version": str(target_version)}},
                    "upsert": True,
                },
                required=False,
            )
        )

        return all_actions, failed_conditions

    def upgrade(
        self,
        from_version: str | None = None,
        target_version: str | None = None,
        force: bool = False,
    ) -> bool:
        """Perform the database upgrade

        This method will check if an upgrade is needed from the given version
        to the target version and execute the necessary upgrade functions.
        If no target version is provided, the current version of the package
        is used.

        Parameters
        ----------
        from_version
            The version from which to start the upgrade. If ``None``, the current version in the database is used.
        target_version
            The target version of the upgrade. If ``None``, the current version of the package is used.
        force
            Perform the upgrade even if the conditions marked as 'skippable' are not satisfied.

        Returns
        -------
        bool
            True if the upgrade was performed.
        """
        db_version = (
            parse_version(from_version)
            if from_version
            else self.job_controller.get_current_db_version()
        )
        target_version = (
            parse_version(target_version) if target_version else self.current_version
        )
        if db_version >= target_version:
            logger.info("Database is already at the target version")
            return False

        versions_needing_upgrade = self.collect_upgrades(db_version, target_version)

        if failed_conditions := self.check_upgrade_conditions(
            versions_needing_upgrade, force=force
        ):
            err = ["Some upgrade conditions were not satisfied:"]
            for vv, failed_cond in failed_conditions:
                err.append(
                    f" - {failed_cond['condition'].description} (for version {vv}): {failed_cond['message']}"
                )
            logger.error("\n".join(err))
            return False

        logger.info(f"Starting upgrade from version {db_version} to {target_version}")

        for version in versions_needing_upgrade:
            with self.open_transaction() as session:
                upgrade_func = self._upgrade_registry[version]
                logger.info(f"Applying upgrade to version {version}")
                upgrade_func(self.job_controller, session=session)
                self.update_db_version(version, session)

        # update the full environment reference and versions
        logger.info("Updating database information")
        self.job_controller.update_version_information(
            jobflow_remote_version=target_version
        )

        logger.info("Database upgrade completed successfully")
        return True

    @contextlib.contextmanager
    def open_transaction(self):
        """
        Open a transaction for the queue DB in the jobstore if it is supported.
        Does nothing and yields None if transactions are not supported
        """
        if self.job_controller.queue_supports_transactions:
            with (
                self.job_controller.db.client.start_session() as session,
                session.start_transaction(),
            ):
                yield session
        else:
            yield None


@DatabaseUpgrader.register_upgrade("0.1.5")
def upgrade_to_0_1_5(
    job_controller: JobController,
    session: ClientSession | None = None,
    dry_run: bool = False,
) -> list[UpgradeAction]:
    actions = []
    action = UpgradeAction(
        description="Create a document for the running runner in the auxiliary collection",
        collection="auxiliary",
        action_type="update",
        details={
            "filter": {"running_runner": {"$exists": True}},
            "update": {"$set": {"running_runner": None}},
            "upsert": True,
            "required": True,
        },
    )

    if not dry_run:
        job_controller.auxiliary.find_one_and_update(
            filter=action.details["filter"],
            update=action.details["update"],
            upsert=action.details["upsert"],
            session=session,
        )

    actions.append(action)
    return actions


def check_batches_in_auxiliary_legacy(
    job_controller: JobController, condition: UpgradeCondition
) -> dict | None:
    batches_docs = list(
        job_controller.auxiliary.find({"batch_processes": {"$exists": True}}).limit(2)
    )
    if len(batches_docs) == 0:
        return None
    if len(batches_docs) > 1:
        raise RuntimeError(
            "More than one document with batch processes found in the auxiliary collection."
        )
    batch_doc = batches_docs[0]
    if batch_doc["batch_processes"] is None:
        return None
    count = 0
    for batch_processes_dict in batch_doc["batch_processes"].values():
        count += len(batch_processes_dict)
    if count == 0:
        return None
    msg = (
        f"Found {count} batche(s) in auxiliary collection (legacy batches management)."
        " If there were batch jobs being executed at the time of the upgrade of version it"
        " preferable to downgrade to the previous version of jobflow-remote and let those "
        " jobs complete before upgrading again jobflw-remote and running `jf admin upgrade`"
    )
    return {
        "condition": condition,
        "message": msg,
        "count": count,
    }


upgrade_conditions_for_1_0 = [
    UpgradeCondition(
        description="There should not be any batch process in the auxiliary collection (old batch management)",
        check_func=check_batches_in_auxiliary_legacy,
        skippable=True,
    ),
    NoDocumentsIn(
        collection="batches",
        query={"batch_state": {"$in": ["SUBMITTED", "RUNNING"]}},
    ),
]


@DatabaseUpgrader.register_upgrade("1.0", upgrade_conditions=upgrade_conditions_for_1_0)
def upgrade_to_1_0(
    job_controller: JobController,
    session: ClientSession | None = None,
    dry_run: bool = False,
) -> list[UpgradeAction]:
    actions = []
    action = UpgradeAction(
        description="Update all TERMINATED job states to RUN_FINISHED",
        collection="jobs",
        action_type="update",
        details={
            "filter": {"state": "TERMINATED"},
            "update": {"$set": {"state": "RUN_FINISHED"}},
            "upsert": False,
            "required": True,
        },
    )

    if not dry_run:
        job_controller.jobs.update_many(
            filter=action.details["filter"],
            update=action.details["update"],
            upsert=action.details["upsert"],
            session=session,
        )

    actions.append(action)

    action = UpgradeAction(
        description="Update all TERMINATED job previous states to RUN_FINISHED",
        collection="jobs",
        action_type="update",
        details={
            "filter": {"previous_state": "TERMINATED"},
            "update": {"$set": {"previous_state": "RUN_FINISHED"}},
            "upsert": False,
            "required": True,
        },
    )

    if not dry_run:
        job_controller.jobs.update_many(
            filter=action.details["filter"],
            update=action.details["update"],
            upsert=action.details["upsert"],
            session=session,
        )

    actions.append(action)

    action = UpgradeAction(
        description="Remove the batches related document from the auxiliary collection",
        collection="auxiliary",
        action_type="delete",
        details={
            "filter": {"batch_processes": {"$exists": True}},
            "required": True,
        },
    )

    if not dry_run:
        job_controller.auxiliary.delete_one(
            filter=action.details["filter"],
            session=session,
        )

    actions.append(action)

    action = UpgradeAction(
        description="Move the 'terminated' directory to 'run_finished' on all batch workers",
        collection="NO_COLLECTION",
        action_type="Filesystems's move of directories on batch workers",
        details={
            "src": "'terminated' directories in the <JOBS_HANDLE_DIR> of each batch worker",
            "dst": "'run_finished' directories in the <JOBS_HANDLE_DIR> of each batch worker",
        },
    )

    if not dry_run:
        for worker_config in job_controller.project.workers.values():
            if worker_config.is_batch:
                host = worker_config.get_host()
                terminated_dir = worker_config.batch.jobs_handle_dir / "terminated"
                if host.exists(terminated_dir):
                    host.move(
                        terminated_dir,
                        worker_config.batch.jobs_handle_dir / "run_finished",
                    )

    actions.append(action)

    return actions
