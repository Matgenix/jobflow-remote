from __future__ import annotations

from typing import TYPE_CHECKING

from jobflow_remote.config.manager import ConfigManager

if TYPE_CHECKING:
    from jobflow.core.store import JobStore


def get_jobstore(
    project_name: str | None = None, jobstore_name: str | None = None
) -> JobStore:
    """
    Helper function to get the jobstore in a project.

    Parameters
    ----------
    project_name
        Name of the project or None to use the one from the settings.
    jobstore_name
        The name of the optional jobstore to return. If None the default JobStore.

    Returns
    -------
    A JobStore
    """
    cm = ConfigManager(warn=False)
    project = cm.get_project(project_name=project_name)
    return project.get_jobstore(jobstore_name)
