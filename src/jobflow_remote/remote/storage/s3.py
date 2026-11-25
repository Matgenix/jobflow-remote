from __future__ import annotations

import io
import logging
import os
import traceback
from pathlib import Path
from typing import Any

from jobflow_remote.remote.storage.base import BaseCloudStorage, StorageError

logger = logging.getLogger(__name__)

_DELETE_BATCH_SIZE = 1000


def _normalize_posix_rel(raw: str) -> str:
    """Normalize a POSIX-style S3 key path, rejecting any ``..`` traversal."""
    parts: list[str] = []
    for part in raw.replace("\\", "/").split("/"):
        if part == "..":
            raise ValueError(f"Path must not contain '..' segments: '{raw}'")
        if part and part != ".":
            parts.append(part)
    return "/".join(parts)


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Parse ``s3://<bucket>/<key>`` into ``(bucket, normalized_key)``."""
    if not uri.startswith("s3://"):
        raise ValueError(
            f"Unsupported cloud storage URI '{uri}'. Expected 's3://<bucket>/<prefix>'."
        )
    parts = uri[5:].split("/", 1)
    bucket = parts[0].strip()
    key = _normalize_posix_rel(parts[1] if len(parts) > 1 else "")
    return bucket, key


def _s3_error_code(exc: Exception) -> str:
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


class S3Storage(BaseCloudStorage):
    """Amazon S3 implementation of :class:`BaseCloudStorage`.

    Parameters
    ----------
    bucket
        Name of the S3 bucket.
    prefix
        Optional key prefix prepended to non-URI paths (``s3://`` URIs must
        already fall under this prefix).
    region_name
        AWS region name (falls back to standard boto3 resolution if ``None``).
    profile_name
        AWS credentials profile name.
    endpoint_url
        Optional custom S3 endpoint URL (for S3-compatible stores or local testing).
    """

    def __init__(
        self,
        bucket: str,
        prefix: str = "",
        region_name: str | None = None,
        profile_name: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        if not isinstance(bucket, str) or not bucket.strip():
            raise ValueError("bucket must be a non-empty string.")
        self.bucket = bucket.strip()
        self.prefix = _normalize_posix_rel(prefix)
        self.region_name = region_name
        self.profile_name = profile_name
        self.endpoint_url = endpoint_url
        self._client: Any = None
        self._boto_errors: tuple[type[Exception], ...] | None = None

    def _get_client_and_errors(self) -> tuple[Any, tuple[type[Exception], ...]]:
        if self._client is None or self._boto_errors is None:
            try:
                import boto3
                from boto3.exceptions import Boto3Error
                from botocore.exceptions import BotoCoreError, ClientError
            except ModuleNotFoundError as exc:
                msg = (
                    "boto3 is required for S3Storage. "
                    "Install it with 'pip install jobflow-remote[aws]'."
                )
                raise ImportError(msg) from exc

            self._boto_errors = (ClientError, BotoCoreError, Boto3Error)
            if self._client is None:
                session = boto3.Session(
                    region_name=self.region_name,
                    profile_name=self.profile_name,
                )
                self._client = session.client("s3", endpoint_url=self.endpoint_url)
        return self._client, self._boto_errors

    def _to_key(self, path: str | Path) -> str:
        raw = str(path).replace("\\", "/")
        if raw.startswith("s3://"):
            uri_bucket, uri_key = parse_s3_uri(raw)
            if uri_bucket != self.bucket:
                raise ValueError(
                    f"S3 URI bucket '{uri_bucket}' does not match storage bucket '{self.bucket}'."
                )
            if (
                self.prefix
                and uri_key != self.prefix
                and not uri_key.startswith(f"{self.prefix}/")
            ):
                raise ValueError(
                    f"S3 URI key '{uri_key}' is outside storage prefix '{self.prefix}'."
                )
            return uri_key

        norm = _normalize_posix_rel(raw)
        if not norm:
            return self.prefix
        if self.prefix:
            return f"{self.prefix}/{norm}"
        return norm

    def get_uri(self, path: str | Path) -> str:
        key = self._to_key(path)
        if key:
            return f"s3://{self.bucket}/{key}"
        return f"s3://{self.bucket}"

    def connect(self) -> None:
        self._get_client_and_errors()

    def close(self) -> bool:
        self._client = None
        return True

    @property
    def is_connected(self) -> bool:
        return self._client is not None

    def put(self, src: str | Path | io.BytesIO, dst: str | Path) -> None:
        key = self._to_key(dst)
        if not key:
            raise ValueError("Destination S3 key must not be empty.")
        client, boto_errors = self._get_client_and_errors()
        try:
            if isinstance(src, io.BytesIO):
                src.seek(0)
                client.upload_fileobj(src, self.bucket, key)
            else:
                client.upload_file(str(src), self.bucket, key)
        except boto_errors as exc:
            raise StorageError(
                f"Failed to upload to s3://{self.bucket}/{key}: {exc}"
            ) from exc

    def get(self, src: str | Path, dst: str | Path) -> None:
        key = self._to_key(src)
        if not key:
            raise ValueError("Source S3 key must not be empty.")
        client, boto_errors = self._get_client_and_errors()
        dst_path = Path(dst)
        existed_before = dst_path.exists()
        try:
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(self.bucket, key, str(dst_path))
        except boto_errors as exc:
            # download_file can create an empty file before raising; remove it if newly created.
            if not existed_before and dst_path.is_file():
                dst_path.unlink(missing_ok=True)
            if _s3_error_code(exc) in {"404", "NoSuchKey", "NotFound"}:
                raise FileNotFoundError(
                    f"S3 object s3://{self.bucket}/{key} does not exist."
                ) from exc
            raise StorageError(
                f"Failed to download s3://{self.bucket}/{key}: {exc}"
            ) from exc

    def exists(self, path: str | Path) -> bool:
        key = self._to_key(path)
        client, boto_errors = self._get_client_and_errors()
        try:
            if not key:
                try:
                    client.head_bucket(Bucket=self.bucket)
                except boto_errors as exc:
                    if _s3_error_code(exc) in {"404", "NoSuchBucket", "NotFound"}:
                        return False
                    raise
                return True

            try:
                client.head_object(Bucket=self.bucket, Key=key)
            except boto_errors as exc:
                if _s3_error_code(exc) not in {"404", "NoSuchKey", "NotFound"}:
                    raise
            else:
                return True

            resp = client.list_objects_v2(
                Bucket=self.bucket, Prefix=f"{key}/", MaxKeys=1
            )
            return bool(resp.get("KeyCount", 0) > 0)
        except boto_errors as exc:
            raise StorageError(
                f"Failed to check s3://{self.bucket}/{key}: {exc}"
            ) from exc

    def listdir(self, path: str | Path) -> list[str]:
        key = self._to_key(path)
        prefix = f"{key}/" if key else ""
        client, boto_errors = self._get_client_and_errors()
        names: list[str] = []
        seen: set[str] = set()
        kwargs: dict[str, Any] = {
            "Bucket": self.bucket,
            "Prefix": prefix,
            "Delimiter": "/",
        }
        try:
            while True:
                resp = client.list_objects_v2(**kwargs)
                for obj in resp.get("Contents", []):
                    rel = obj["Key"][len(prefix) :]
                    if rel and rel not in seen:
                        seen.add(rel)
                        names.append(rel)
                for common in resp.get("CommonPrefixes", []):
                    rel = common["Prefix"][len(prefix) :].rstrip("/")
                    if rel and rel not in seen:
                        seen.add(rel)
                        names.append(rel)
                if not resp.get("IsTruncated"):
                    break
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except boto_errors as exc:
            raise StorageError(
                f"Failed to list s3://{self.bucket}/{prefix}: {exc}"
            ) from exc
        return names

    def remove(self, path: str | Path) -> None:
        key = self._to_key(path)
        if not key:
            raise ValueError("S3 key to remove must not be empty.")
        client, boto_errors = self._get_client_and_errors()
        try:
            client.delete_object(Bucket=self.bucket, Key=key)
        except boto_errors as exc:
            raise StorageError(
                f"Failed to delete s3://{self.bucket}/{key}: {exc}"
            ) from exc

    def rmtree(self, path: str | Path, raise_on_error: bool = False) -> bool:
        key = self._to_key(path)
        if not key or (self.prefix and not key.startswith(f"{self.prefix}/")):
            msg = "Refusing to recursively delete the S3 bucket or storage prefix root."
            if raise_on_error:
                raise ValueError(msg)
            logger.error(msg)
            return False

        prefix = f"{key}/"
        client, boto_errors = self._get_client_and_errors()
        keys_to_delete: list[dict[str, str]] = []
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}
        try:
            exact = client.list_objects_v2(Bucket=self.bucket, Prefix=key, MaxKeys=1)
            keys_to_delete.extend(
                {"Key": key} for obj in exact.get("Contents", []) if obj["Key"] == key
            )

            while True:
                resp = client.list_objects_v2(**kwargs)
                keys_to_delete.extend(
                    {"Key": obj["Key"]} for obj in resp.get("Contents", [])
                )
                if not resp.get("IsTruncated"):
                    break
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]

            delete_errors: list[Any] = []
            for idx in range(0, len(keys_to_delete), _DELETE_BATCH_SIZE):
                chunk = keys_to_delete[idx : idx + _DELETE_BATCH_SIZE]
                del_resp = client.delete_objects(
                    Bucket=self.bucket, Delete={"Objects": chunk, "Quiet": True}
                )
                if del_resp.get("Errors"):
                    delete_errors.extend(del_resp["Errors"])
        except boto_errors as exc:
            if raise_on_error:
                raise StorageError(
                    f"Error deleting s3://{self.bucket}/{prefix}: {exc}"
                ) from exc
            logger.warning(f"Error deleting s3://{self.bucket}/{prefix}: {exc}")
            return False

        if delete_errors:
            msg = f"Errors deleting objects under s3://{self.bucket}/{prefix}: {delete_errors}"
            if raise_on_error:
                raise StorageError(msg)
            logger.warning(msg)
            return False
        return True

    def upload_dir(
        self,
        local_dir: str | Path,
        remote_dir: str | Path,
        skip_files: set[str] | None = None,
    ) -> None:
        base_local = Path(local_dir).resolve()
        if not base_local.is_dir():
            raise FileNotFoundError(f"Local directory does not exist: {base_local}")
        base_key = self._to_key(remote_dir)
        client, boto_errors = self._get_client_and_errors()
        try:
            for root, _dirs, files in os.walk(base_local):
                for fname in files:
                    fpath = Path(root) / fname
                    # Skip broken symlinks and special files (sockets/FIFOs).
                    if not fpath.is_file():
                        continue
                    rel = fpath.relative_to(base_local).as_posix()
                    if skip_files and rel in skip_files:
                        continue
                    key = f"{base_key}/{rel}" if base_key else rel
                    client.upload_file(str(fpath), self.bucket, key)
        except boto_errors as exc:
            raise StorageError(
                f"Failed to upload directory {base_local} to s3://{self.bucket}/{base_key}: {exc}"
            ) from exc

    def download_dir(self, remote_dir: str | Path, local_dir: str | Path) -> None:
        base_local = Path(local_dir).resolve()
        base_key = self._to_key(remote_dir)
        prefix = f"{base_key}/" if base_key else ""
        client, boto_errors = self._get_client_and_errors()
        kwargs: dict[str, Any] = {"Bucket": self.bucket, "Prefix": prefix}
        try:
            base_local.mkdir(parents=True, exist_ok=True)
            while True:
                resp = client.list_objects_v2(**kwargs)
                for obj in resp.get("Contents", []):
                    key = obj["Key"]
                    rel = key[len(prefix) :]
                    if not rel or rel.endswith("/"):
                        continue
                    target = (base_local / rel).resolve()
                    if base_local not in target.parents:
                        raise StorageError(
                            f"Refusing to write S3 key '{key}' outside target directory '{base_local}'."
                        )
                    target.parent.mkdir(parents=True, exist_ok=True)
                    client.download_file(self.bucket, key, str(target))
                if not resp.get("IsTruncated"):
                    break
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        except boto_errors as exc:
            raise StorageError(
                f"Failed to download s3://{self.bucket}/{prefix} to {base_local}: {exc}"
            ) from exc

    def test(self) -> str | None:
        try:
            client, _ = self._get_client_and_errors()
            client.head_bucket(Bucket=self.bucket)
        except Exception:
            return (
                f"Error accessing S3 bucket '{self.bucket}':\n{traceback.format_exc()}"
            )
        return None

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, S3Storage):
            return False
        return (
            self.bucket == other.bucket
            and self.prefix == other.prefix
            and self.region_name == other.region_name
            and self.profile_name == other.profile_name
            and self.endpoint_url == other.endpoint_url
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.bucket,
                self.prefix,
                self.region_name,
                self.profile_name,
                self.endpoint_url,
            )
        )
