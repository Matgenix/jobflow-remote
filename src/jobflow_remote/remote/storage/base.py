from __future__ import annotations

import abc
from typing import TYPE_CHECKING

from monty.json import MSONable

if TYPE_CHECKING:
    import io
    from pathlib import Path


class StorageError(Exception):
    """Exception raised for cloud object storage failures."""


class BaseCloudStorage(MSONable, abc.ABC):
    """Base class for cloud object storage backends."""

    @abc.abstractmethod
    def put(self, src: str | Path | io.BytesIO, dst: str | Path) -> None:
        """Upload a local file or in-memory buffer to ``dst``."""

    @abc.abstractmethod
    def get(self, src: str | Path, dst: str | Path) -> None:
        """Download object ``src`` to local file path ``dst``.

        Raises ``FileNotFoundError`` if ``src`` does not exist.
        """

    @abc.abstractmethod
    def exists(self, path: str | Path) -> bool:
        """Return ``True`` if an object or non-empty prefix exists at ``path``."""

    @abc.abstractmethod
    def listdir(self, path: str | Path) -> list[str]:
        """List immediate file and subfolder names under ``path``."""

    @abc.abstractmethod
    def remove(self, path: str | Path) -> None:
        """Delete a single object at ``path``."""

    @abc.abstractmethod
    def rmtree(self, path: str | Path, raise_on_error: bool = False) -> bool:
        """Recursively delete all objects under prefix ``path``."""

    def mkdir(
        self, directory: str | Path, recursive: bool = True, exist_ok: bool = True
    ) -> bool:
        """No-op for flat object stores; prefixes are created on upload."""
        return True

    @abc.abstractmethod
    def upload_dir(
        self,
        local_dir: str | Path,
        remote_dir: str | Path,
        skip_files: set[str] | None = None,
    ) -> None:
        """Recursively upload all files in ``local_dir`` to ``remote_dir``."""

    @abc.abstractmethod
    def download_dir(self, remote_dir: str | Path, local_dir: str | Path) -> None:
        """Recursively download all objects under ``remote_dir`` into ``local_dir``."""

    @abc.abstractmethod
    def get_uri(self, path: str | Path) -> str:
        """Return the storage URI (e.g. ``s3://bucket/key``) for ``path``."""

    @abc.abstractmethod
    def test(self) -> str | None:
        """Verify access to the underlying storage. Return ``None`` if OK, else an error message."""

    def connect(self) -> None:
        """Initialize the underlying client if needed."""

    def close(self) -> bool:
        """Release any cached client state."""
        return True

    @property
    def is_connected(self) -> bool:
        """Whether the storage client is ready."""
        return True
