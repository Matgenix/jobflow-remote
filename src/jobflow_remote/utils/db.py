from __future__ import annotations

import copy
import logging
import time
import warnings
from collections import defaultdict
from datetime import datetime

from jobflow.utils import suuid
from pymongo import ReturnDocument

from jobflow_remote.utils.data import deep_merge_dict

logger = logging.getLogger(__name__)


class MongoLock:
    LOCK_KEY = "lock_id"
    LOCK_TIME_KEY = "lock_time"

    def __init__(
        self,
        collection,
        filter,
        update=None,
        break_lock=False,
        lock_id=None,
        sleep=None,
        max_wait=600,
        projection=None,
        get_locked_doc=False,
        **kwargs,
    ):
        self.collection = collection
        self.filter = filter or {}
        self.update = update
        self.break_lock = break_lock
        self.locked_document = None
        self.unavailable_document = None
        self.lock_id = lock_id or suuid()
        self.kwargs = kwargs
        self.update_on_release: dict = {}
        self.sleep = sleep
        self.max_wait = max_wait
        self.projection = projection
        self.get_locked_doc = get_locked_doc

    def get_lock_time(self, d: dict):
        return d.get(self.LOCK_TIME_KEY)

    def get_lock_id(self, d: dict):
        return d.get(self.LOCK_KEY)

    def acquire(self):
        # Set the lock expiration time
        now = datetime.utcnow()
        db_filter = copy.deepcopy(self.filter)

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
        update = defaultdict(dict)
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
            update = [dict(update)]

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
                else:
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

    def release(self, exc_type, exc_val, exc_tb):
        # Release the lock by removing the unique identifier and lock expiration time
        update = {"$set": {self.LOCK_KEY: None, self.LOCK_TIME_KEY: None}}
        # TODO maybe set on release only if no exception was raised?
        if self.update_on_release:
            update = deep_merge_dict(update, self.update_on_release)
        logger.debug(f"release lock with update: {update}")
        # TODO if failed to release the lock maybe retry before failing
        result = self.collection.update_one(
            {"_id": self.locked_document["_id"], self.LOCK_KEY: self.lock_id},
            update,
            upsert=False,
        )

        # Check if the lock was successfully released
        if result.modified_count == 0:
            msg = f"Could not release lock for document {self.locked_document['_id']}"
            warnings.warn(msg, stacklevel=2)

        self.locked_document = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.locked_document:
            self.release(exc_type, exc_val, exc_tb)


class LockedDocumentError(Exception):
    """
    Exception to signal a problem when locking the document
    """


class JobLockedError(LockedDocumentError):
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
    @classmethod
    def from_flow_doc(cls, doc: dict, additional_msg: str | None = None):
        lock_id = doc[MongoLock.LOCK_KEY]
        lock_date = doc[MongoLock.LOCK_TIME_KEY]
        date_str = lock_date.isoformat(timespec="seconds") if lock_date else None
        msg = f"Flow with uuid {doc['uuid']} is locked with lock_id {lock_id} since {date_str} UTC."
        if additional_msg:
            msg += " " + additional_msg
        return cls(msg)
