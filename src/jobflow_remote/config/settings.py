from __future__ import annotations

from pathlib import Path

from pydantic import BaseSettings


class JobflowRemoteSettings(BaseSettings):
    projects_folder: str = Path("~/.jfremote").expanduser().as_posix()
    current_project: str = None

    class Config:
        """Pydantic config settings."""

        env_prefix = "jfremote_"
