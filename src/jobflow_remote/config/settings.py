from __future__ import annotations

from pathlib import Path

from pydantic import BaseSettings, validator


class JobflowRemoteSettings(BaseSettings):
    projects_folder: str = Path("~/.jfremote").expanduser().as_posix()
    daemon_folder: str = ""
    project: str = None

    @validator("daemon_folder", always=True)
    def get_daemon_folder(cls, daemon_folder: str, values: dict) -> str:
        """
        Validator to set the default of daemon_folder based on projects_folder
        """
        if daemon_folder == "" and "projects_folder" in values:
            return str(Path(values["projects_folder"], "daemon"))
        return daemon_folder

    class Config:
        """Pydantic config settings."""

        env_prefix = "jfremote_"
