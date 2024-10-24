from __future__ import annotations

import copy
import logging
import os
import re
import subprocess
import time
import warnings
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from shutil import which
from typing import TYPE_CHECKING, Any

import bson
from maggma.stores.mongolike import MongoStore, MongoURIStore
from monty.io import zopen
from pymongo import ReturnDocument
from pymongo.errors import PyMongoError

from jobflow_remote.utils.data import deep_merge_dict, suuid

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from pymongo.collection import Collection

logger = logging.getLogger(__name__)


class MongoLock:
    """
    Context manager to lock a document in a MongoDB database.

    Main characteristics and functionalities:
        * Lock is acquired by setting a lock_id and lock_time value in the
          locked document.
        * Filter the document to select based on a query and sorting.
          It uses find_one_and_update, thus resulting in a single document locked.
        * Can wait for a lock to be released.
        * Can forcibly break an existing lock
        * Can return the locked document even if the lock could not be acquired
          (useful for determining if a document is locked or no document matches
          the query)
        * Accepts all the arguments that can be passed to find_one_and_update
        * A custom update can be performed on the document when acquiring the lock.
        * Allows to pass properties that will be set in the document at the moment
          of releasing the lock.
        * Lock id value can be customized. If not a randomly generated uuid is used.

    Examples
    --------
    Trying to acquire the lock on a document based on the state
    >>> with MongoLock(collection, {"state": "READY"}) as lock:
    ...     print(lock.locked_document["state"])
    READY

    If lock cannot be acquired (no document matching filter or that
    document is locked) the `lock.locked_document` is None.

    >>> with MongoLock(collection, {"state": "READY"}) as lock:
    ...     print(lock.locked_document)
    None

    Wait for 60 seconds in case the required document is already locked.
    Check the status every 10 seconds. If lock cannot be acquired
    locked_document is None

    >>> with MongoLock(
            collection,
            {"uuid": "5b84228b-d019-47fe-b0a0-564b36aa85ed"},
            sleep=10,
            max_wait=60,
        ) as lock:
    ...     print(lock.locked_document)
    None

    In case lock cannot be acquired expose the locked document

    >>> with MongoLock(
            collection,
            {"uuid": "5b84228b-d019-47fe-b0a0-564b36aa85ed"},
            get_locked_doc=True,
        ) as lock:
    ...     print(lock.locked_document)
    None
    ...     print(lock.unavailable_document['lock_id'])
    8d68404f-c77a-461b-859c-40bb0af1979f

    Set values in the document upon lock release

    >>> with MongoLock(collection, {"state": "READY"}) as lock:
    ...     if lock.locked_document:
    ...         # Perform some operations based on the job...
    ...         lock.update_on_release = {"$set": {"state": "CHECKED_OUT"}}

    Delete the locked document upon lock release

    >>> with MongoLock(collection, {"state": "READY"}) as lock:
    ...     if lock.locked_document:
    ...         # decide if the document should be deleted
    ...         lock.delete_on_release = True

    """

    LOCK_KEY = "lock_id"
    LOCK_TIME_KEY = "lock_time"

    def __init__(
        self,
        collection: Collection,
        filter: Mapping[str, Any],  # noqa: A002
        update: Mapping[str, Any] | None = None,
        break_lock: bool = False,
        lock_id: str | None = None,
        sleep: int | None = None,
        max_wait: int = 600,
        projection: Mapping[str, Any] | Iterable[str] | None = None,
        get_locked_doc: bool = False,
        **kwargs,
    ) -> None:
        """
        Parameters
        ----------
        collection
            The MongoDB collection containing the document to lock.
        filter
            A MongoDB query to select the document.
        update
            A dictionary that will be passed to find_one_and_update to update the
            locked document at the moment of acquiring the lock.
        break_lock
            True if the context manager is allowed to forcibly break a lock.
        lock_id
            The is used for the lock in the document. If None a randomly generated
            uuid will be used.
        sleep
            The amount of second to sleep between consecutive checks while waiting
            for a lock to be released.
        max_wait
            The amount of seconds to wait for a lock to be released.
        projection
            The projection passed to the find_one_and_update that locks the document.
        get_locked_doc
            If True, if the lock cannot be acquired because the document matching
            the filter is already locked, the locked document will be fetched and
            set in the unavailable_document attribute.
        kwargs
            All the other args are passed to find_one_and_update.
        """
        self.collection = collection
        self.filter = filter or {}
        self.update = update
        self.break_lock = break_lock
        self.locked_document = None
        self.unavailable_document = None
        self.lock_id = lock_id or suuid()
        self.kwargs = kwargs
        self._update_on_release: dict | list = {}
        self._delete_on_release: bool = False
        self.sleep = sleep
        self.max_wait = max_wait
        self.projection = projection
        self.get_locked_doc = get_locked_doc

    @property
    def update_on_release(self) -> dict | list:
        """
        The update_on_release value.

        Returns:
            dict | list: The value of update_on_release.
        """
        return self._update_on_release

    @update_on_release.setter
    def update_on_release(self, value: dict | list):
        """
        Set the value of update_on_release.

        If set its value will be used to update the document with additional
        properties upon lock release.

        For example:
        lock.update_on_release = {"$set": {"state": "CHECKED_OUT"}}

        Cannot be set together with delete_on_release.

        Parameters
        ----------
        value : dict | list
            The value to set for update_on_release.
        """
        if self.delete_on_release:
            raise ValueError(
                "delete_on_release and update_on_release cannot be set simultaneously"
            )
        self._update_on_release = value

    @property
    def delete_on_release(self) -> bool:
        """
        The delete_on_release property.

        Returns:
            bool: Whether the document will be deleted upon lock release.
        """
        return self._delete_on_release

    @delete_on_release.setter
    def delete_on_release(self, value: bool):
        """
        Set the value of delete_on_release.

        If True the document will be deleted upon lock release.

        For example:
        lock.delete_on_release = True

        Cannot be set together with update_on_release.

        Parameters
        ----------
        value : bool
            The value to set for delete_on_release.
        """
        if self.update_on_release:
            raise ValueError(
                "delete_on_release and update_on_release cannot be set simultaneously"
            )
        self._delete_on_release = value

    @property
    def is_locked(self) -> bool:
        """Return whether the document was locked before trying to acquire the lock.

        Notes
        -----
        This method should be used only inside the "with" context.
        """
        return self.locked_document is None

    @classmethod
    def get_lock_time(cls, d: dict):
        """Get the time the document was locked on a dictionary."""
        return d.get(cls.LOCK_TIME_KEY)

    @classmethod
    def get_lock_id(cls, d: dict):
        """Get the lock id on a dictionary."""
        return d.get(cls.LOCK_KEY)

    def acquire(self) -> None:
        """Acquire the lock."""
        # Set the lock expiration time
        now = datetime.utcnow()
        db_filter = copy.deepcopy(dict(self.filter))

        projection = self.projection
        # if projecting always get the lock as well
        if projection:
            projection = list(projection)
            projection.extend([self.LOCK_KEY, self.lock_id])

        # Modify the filter if the document should not be fetched if
        # the lock cannot be acquired. Otherwise, keep the original filter.
        if not self.break_lock and not self.sleep and not self.get_locked_doc:
            db_filter.update({self.LOCK_KEY: None})

        # Prepare the update to be performed when acquiring the lock.
        # A combination of the input update and the setting of the lock.
        lock_set = {self.LOCK_KEY: self.lock_id, self.LOCK_TIME_KEY: now}
        update: dict[str, dict] = defaultdict(dict)
        if self.update:
            update.update(copy.deepcopy(self.update))

        update["$set"].update(lock_set)

        # If the document should be fetched even if the lock could not be acquired
        # the updates should be made conditional.
        # Note: sleep needs to fetch the document, otherwise it is impossible to
        # determine if the filter did not return any document or if the document
        # was locked.
        if (self.sleep or self.get_locked_doc) and not self.break_lock:
            for operation, dict_vals in update.items():
                for k, v in dict_vals.items():
                    cond = {
                        "$cond": {
                            "if": {"$gt": [f"${self.LOCK_KEY}", None]},
                            "then": f"${k}",
                            "else": v,
                        }
                    }
                    update[operation][k] = cond
            update = [dict(update)]  # type: ignore[assignment]

        # Try to acquire the lock by updating the document with a unique identifier
        # and the lock expiration time
        logger.debug(f"try acquiring lock with filter: {db_filter}")
        t0 = time.time()
        while True:
            result = self.collection.find_one_and_update(
                db_filter,
                update,
                upsert=False,
                return_document=ReturnDocument.AFTER,
                **self.kwargs,
            )

            if result:
                lock_acquired = self.get_lock_id(result) == self.lock_id
                if lock_acquired:
                    self.locked_document = result
                    break
                # if the lock could not be acquired optionally sleep or
                # exit if waited for enough time.
                if self.sleep and (time.time() - t0) < self.max_wait:
                    logger.debug("sleeping")
                    time.sleep(self.sleep)
                else:
                    self.unavailable_document = result
                    break
            else:
                # If no document the conditions could not be met.
                # Either the requested filter does not find match a document
                # or those fitting are locked.
                break

    def release(self, exc_type, exc_val, exc_tb) -> None:
        """Release the lock."""

        # TODO if failed to release the lock maybe retry before failing
        if self.locked_document is None:
            return

        # Release the lock by removing the unique identifier and lock expiration time
        update: list | dict = {"$set": {self.LOCK_KEY: None, self.LOCK_TIME_KEY: None}}
        # TODO maybe set on release only if no exception was raised?
        if self.update_on_release:
            # if an exception raised inside the context manager do not update the document
            if exc_type is not None:
                logger.warning(
                    f"A {type(exc_type)} exception was raised while the document was locked. "
                    f"The update_on_release {self.update_on_release} will not be applied."
                )
            elif isinstance(self.update_on_release, list):
                update = [update, *self.update_on_release]
            else:
                update = deep_merge_dict(update, self.update_on_release)
        logger.debug(f"release lock with update: {update}")

        # if an exception raised inside the context manager do not delete the document
        if self.delete_on_release and exc_type is None:
            result = self.collection.delete_one(
                {"_id": self.locked_document["_id"], self.LOCK_KEY: self.lock_id}
            )
            if result.deleted_count == 0:
                raise RuntimeError("Could not delete the locked document upon release")
        else:
            if self.delete_on_release and exc_type is not None:
                logger.warning(
                    f"A {type(exc_type)} exception was raised while the document was locked. "
                    f"The document will not be deleted, as instead requested by delete_on_release."
                )
            result = self.collection.update_one(
                {"_id": self.locked_document["_id"], self.LOCK_KEY: self.lock_id},
                update,
                upsert=False,
            )

            # Check if the lock was successfully released
            if result.modified_count == 0:
                msg = (
                    f"Could not release lock for document {self.locked_document['_id']}"
                )
                warnings.warn(msg, stacklevel=2)

        self.locked_document = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.locked_document:
            self.release(exc_type, exc_val, exc_tb)


class LockedDocumentError(Exception):
    """Exception to signal a problem when locking the document."""


class RunnerLockedError(LockedDocumentError):
    """Exception to signal a problem when locking a Runner document."""

    @classmethod
    def from_runner_doc(cls, doc: dict, additional_msg: str | None = None):
        lock_id = doc[MongoLock.LOCK_KEY]
        lock_date = doc[MongoLock.LOCK_TIME_KEY]
        date_str = lock_date.isoformat(timespec="seconds") if lock_date else None
        msg = f"Runner document is locked with lock_id {lock_id} since {date_str} UTC."
        if additional_msg:
            msg += " " + additional_msg
        return cls(msg)


class JobLockedError(LockedDocumentError):
    """Exception to signal a problem when locking a Job document."""

    @classmethod
    def from_job_doc(cls, doc: dict, additional_msg: str | None = None):
        lock_id = doc[MongoLock.LOCK_KEY]
        lock_date = doc[MongoLock.LOCK_TIME_KEY]
        date_str = lock_date.isoformat(timespec="seconds") if lock_date else None
        msg = f"Job with db_id {doc['db_id']} is locked with lock_id {lock_id} since {date_str} UTC."
        if additional_msg:
            msg += " " + additional_msg
        return cls(msg)


class FlowLockedError(LockedDocumentError):
    """Exception to signal a problem when locking a Flow document."""

    @classmethod
    def from_flow_doc(cls, doc: dict, additional_msg: str | None = None):
        lock_id = doc[MongoLock.LOCK_KEY]
        lock_date = doc[MongoLock.LOCK_TIME_KEY]
        date_str = lock_date.isoformat(timespec="seconds") if lock_date else None
        msg = f"Flow with uuid {doc['uuid']} is locked with lock_id {lock_id} since {date_str} UTC."
        if additional_msg:
            msg += " " + additional_msg
        return cls(msg)


class MissingDocumentError(Exception):
    """Exception to signal that a document is missing from the DB"""


def mongo_operation(
    store: MongoStore | MongoURIStore,
    file_path: str | Path,
    operation: str,
    collection: str | None,
    mongo_bin_path: str | None = None,
    compress: bool = False,
) -> tuple[str, str]:
    """
    Execute a mongo operation (mongodump or mongorestore) on a given store.

    Parameters
    ----------
    store
        The store containing the data to be backed up.
    file_path
        The path of the folder where the backup files are located.
    operation
        The mongo operation to perform. Can be either 'mongodump' or 'mongorestore'.
    collection
        The name of the collection to be backed up. If None the collection defined in
        the store will be used.
    mongo_bin_path
        The path to the folder containing the mongo executable. If None, the executable is
        searched in the PATH.
    compress
        If True, the backup files are compressed.

    Returns
    -------
    tuple[str, str]
        The stdout and stderr of the executed command.
    """
    if operation not in ("mongodump", "mongorestore"):
        raise ValueError(f"Operation {operation} not supported")

    if mongo_bin_path:
        operation = os.path.join(mongo_bin_path, operation)

    if not which(operation):
        raise RuntimeError(
            f"It looks like the command {operation} is not available. Check the path or consider "
            f"installing the mongodb database tools. Alternatively use the pure python implementation."
        )

    # here is not checked with isinstance(), because the current other subclasses will fail
    if type(store) is MongoURIStore:
        cmd = [
            operation,
            "--uri",
            store.uri,
        ]
    elif type(store) is MongoStore:
        cmd = [
            operation,
            "--host",
            store.host,
            "--port",
            str(store.port),
        ]
        if store.username and store.password:
            cmd.extend(["--username", store.username, "--password", store.password])
            if store.auth_source:
                cmd.extend(["--authenticationDatabase", store.auth_source])
        elif store.username or store.password:
            raise ValueError(
                "To use the mongotools the username and password in the queue store should be "
                "either both present or both absent."
            )
    else:
        raise ValueError(
            f"Unsupported store type {type(store).__name__}. Consider using the python version."
        )

    # check this afterwards, since other stores may not have ssh_tunnel attribute
    if store.ssh_tunnel is not None:
        raise NotImplementedError(
            "SSH tunnel is not supported. Consider using the python version."
        )

    cmd.extend(["--db", store.database, "--collection", collection])

    if compress:
        cmd.append("--gzip")

    if operation.endswith("mongodump"):
        cmd.extend(["--out", file_path])
    elif operation.endswith("mongorestore"):
        cmd.append(file_path)

    str_cmd = " ".join(str(s) for s in cmd)
    result = subprocess.run(
        str_cmd, check=True, capture_output=True, text=True, shell=True
    )
    logger.debug(
        f"output during execution of '{str_cmd}'. Stdout: {result.stdout}. Stderr: {result.stderr}"
    )
    return result.stdout, result.stderr


def mongodump_from_store(
    store: MongoStore | MongoURIStore,
    output_path: str | Path,
    collection: str | None = None,
    mongo_bin_path: str | None = None,
    compress: bool = False,
) -> int:
    """
    Use mongodump to dump a collection from a MongoDB store to a file.

    Parameters
    ----------
    store
        The store containing the data to be backed up.
    output_path
        The path of the folder where the backup files are located.
    collection
        The name of the collection to be backed up. If None the collection defined in
        the store will be used.
    mongo_bin_path
        The path to the folder containing the mongo executable. If None, the executable is
        searched in the PATH.
    compress
        If True, the backup files are compressed.

    Returns
    -------
    int
        The number of documents dumped.
    """
    stdout, stderr = mongo_operation(
        store=store,
        collection=collection,
        file_path=output_path,
        operation="mongodump",
        mongo_bin_path=mongo_bin_path,
        compress=compress,
    )

    # Extract the number of documents dumped
    match = re.search(r"done dumping .*\((\d+) documents*\)", stderr)
    return int(match.group(1)) if match else 0


def mongorestore_to_store(
    store: MongoStore | MongoURIStore,
    input_file: str | Path,
    collection: str | None = None,
    mongo_bin_path: str | None = None,
    compress: bool | None = None,
) -> None:
    """
    Restore a collection from a BSON file using mongorestore.

    Parameters
    ----------
    store
        The store where the data should be restored.
    input_file
        The path of the BSON file containing the data to be restored.
    collection
        The name of the collection to be backed up. If None the collection defined in
        the store will be used.
    mongo_bin_path
        The path to the folder containing the mongo executable. If None, the executable is
        searched in the PATH.
    compress
        If True, the input file is expected to be compressed. If None it will be determined
        based on the file extension.
    """
    if compress is None:
        compress = Path(input_file).name.endswith(".gz")
    mongo_operation(
        store=store,
        collection=collection,
        file_path=input_file,
        operation="mongorestore",
        mongo_bin_path=mongo_bin_path,
        compress=compress,
    )


def pymongo_dump(
    collection: Collection, output_path: str | Path, compress: bool = False
) -> int:
    """
    Dump the contents of a PyMongo collection to a BSON file.

    Parameters
    ----------
    collection
        The PyMongo collection to be dumped.
    output_path
        The path of the folder where the BSON file should be saved.
    compress : bool
        If True, the output file is compressed.

    Returns
    -------
    int
        The number of documents dumped.
    """
    dir_path = Path(output_path) / collection.database.name
    dir_path.mkdir(exist_ok=True, parents=True)
    # add the db name for consistency with mongodump and use the collection name as file name
    file_name = f"{collection.name}.bson"
    if compress:
        file_name += ".gz"
    with zopen(dir_path / file_name, "wb") as f:
        num_documents = 0
        for doc in collection.find():
            f.write(bson.BSON.encode(doc))
            num_documents += 1

    return num_documents


def pymongo_restore(collection: Collection, input_file: str | Path) -> None:
    """
    Restore the contents of a BSON file to a PyMongo collection.

    Parameters
    ----------
    collection
        The PyMongo collection to be dumped.
    input_file
        The path of the BSON file used to restore the data.
    """
    try:
        with zopen(input_file, "rb") as f:
            collection.insert_many(bson.decode_all(f.read()))
    except PyMongoError as e:
        raise RuntimeError(f"Error during PyMongo restore: {e!s}") from e
    except OSError as e:
        raise RuntimeError(f"Error reading from file: {e!s}") from e
