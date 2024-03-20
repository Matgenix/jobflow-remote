from pathlib import Path
from typing import Optional

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from jobflow_remote.config.base import LogLevel

DEFAULT_PROJECTS_FOLDER = Path("~/.jfremote").expanduser().as_posix()

DEFAULT_CONFIG_FILE_PATH = Path("~/.jfremote.yaml").expanduser().as_posix()


class JobflowRemoteSettings(BaseSettings):
    config_file: str = Field(
        DEFAULT_CONFIG_FILE_PATH,
        description="Location of the config file for jobflow remote.",
    )
    projects_folder: str = Field(
        DEFAULT_PROJECTS_FOLDER, description="Location of the projects files."
    )
    project: Optional[str] = Field(None, description="The name of the project used.")
    cli_full_exc: bool = Field(
        False,
        description="If True prints the full stack trace of the exception when raised in the CLI.",
    )
    cli_suggestions: bool = Field(
        True, description="If True prints some suggestions in the CLI commands."
    )
    cli_log_level: LogLevel = Field(
        LogLevel.WARN, description="The level set for logging in the CLI"
    )

    model_config = SettingsConfigDict(env_prefix="jfremote_")

    @model_validator(mode="before")
    @classmethod
    def load_default_settings(cls, values: dict) -> dict:
        """
        Load settings from file or environment variables.

        Loads settings from a root file if available and uses that as defaults in
        place of built-in defaults.

        This allows setting of the config file path through environment variables.
        """
        from monty.serialization import loadfn

        config_file_path: str = values.get("config_file", DEFAULT_CONFIG_FILE_PATH)

        new_values = {}
        if Path(config_file_path).expanduser().exists():
            new_values.update(loadfn(Path(config_file_path).expanduser()))

        new_values.update(values)
        return new_values
