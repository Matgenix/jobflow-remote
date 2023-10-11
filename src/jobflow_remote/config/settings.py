from __future__ import annotations

from pathlib import Path

from pydantic import BaseSettings, Field, root_validator

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
    project: str = Field(None, description="The name of the project used.")
    cli_full_exc: bool = Field(
        False,
        description="If True prints the full stack trace of the exception when raised in the CLI.",
    )
    cli_suggestions: bool = Field(
        True, description="If True prints some suggestions in the CLI commands."
    )

    class Config:
        """Pydantic config settings."""

        env_prefix = "jfremote_"

    @root_validator(pre=True)
    def load_default_settings(cls, values):
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
