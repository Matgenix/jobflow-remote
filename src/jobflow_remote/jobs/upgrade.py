from __future__ import annotations

import contextlib
import functools
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Callable, ClassVar

from packaging.version import Version
from packaging.version import parse as parse_version

import jobflow_remote

if TYPE_CHECKING:
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


class DatabaseUpgrader:
    """
    Object to handle the upgrade of the database between different versions
    """

    _upgrade_registry: ClassVar[dict[Version, Callable]] = {}

    def __init__(self, job_controller: JobController):
        self.job_controller = job_controller
        self.current_version = parse_version(jobflow_remote.__version__)

    @classmethod
    def register_upgrade(cls, version: str):
        """Decorator to register upgrade functions.

        This decorator should be used to register functions that implement the upgrades for each
        version.

        Parameters
        ----------
        version
            The version to register the upgrade function for
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

            cls._upgrade_registry[parse_version(version)] = wrapper
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

    def dry_run(
        self, from_version: str | None = None, target_version: str | None = None
    ) -> list[UpgradeAction]:
        """Simulate the upgrade process and return all actions that would be performed

        Parameters
        ----------
        from_version
            The version from which to start the upgrade. If ``None``, the current version in the database is used.
        target_version
            The target version of the upgrade. If ``None``, the current version of the package is used.

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
            return []

        versions_needing_upgrade = self.collect_upgrades(db_version, target_version)

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

        return all_actions

    def upgrade(
        self, from_version: str | None = None, target_version: str | None = None
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
