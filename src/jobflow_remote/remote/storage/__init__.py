from jobflow_remote.remote.storage.base import BaseCloudStorage, StorageError
from jobflow_remote.remote.storage.s3 import S3Storage

cloud_storage_mapping: dict[str, type[BaseCloudStorage]] = {"s3": S3Storage}

__all__ = ["BaseCloudStorage", "S3Storage", "StorageError", "cloud_storage_mapping"]
