from __future__ import annotations

import copy
import logging
import warnings
from collections import defaultdict
from datetime import datetime, timedelta

from jobflow_remote.utils.data import deep_merge_dict

logger = logging.getLogger(__name__)


class MongoLock:

    LOCK_KEY = "_lock_id"
    LOCK_TIME_KEY = "_lock_time"

    def __init__(
        self,
        collection,
        filter,
        update=None,
        timeout=None,
        break_lock=False,
        lock_id=None,
        **kwargs,
    ):
        self.collection = collection
        self.filter = filter or {}
        self.update = update
        self.timeout = timeout
        self.break_lock = break_lock
        self.locked_document = None
        self.lock_id = lock_id or id(self)
        self.kwargs = kwargs
        self.update_on_release = None

    def acquire(self):
        # Set the lock expiration time
        now = datetime.utcnow()
        db_filter = copy.deepcopy(self.filter)

        if not self.break_lock:
            lock_filter = {self.LOCK_KEY: {"$exists": False}}
            lock_limit = None
            if self.timeout:
                lock_limit = now - timedelta(seconds=self.timeout)
                time_filter = {self.LOCK_TIME_KEY: {"$lt": lock_limit}}
                combined_filter = {"$or": [lock_filter, time_filter]}
                if "$or" in db_filter:
                    db_filter["$and"] = [db_filter, combined_filter]
                else:
                    db_filter.update(combined_filter)
            else:
                db_filter.update(lock_filter)

        lock_set = {self.LOCK_KEY: self.lock_id, self.LOCK_TIME_KEY: now}
        update = defaultdict(dict)
        if self.update:
            update.update(copy.deepcopy(self.update))

        update["$set"].update(lock_set)

        # Try to acquire the lock by updating the document with a unique identifier
        # and the lock expiration time
        logger.debug(f"acquire lock with filter: {db_filter}")
        result = self.collection.find_one_and_update(
            db_filter, update, upsert=False, **self.kwargs
        )

        if result:
            if lock_limit and result[self.LOCK_TIME_KEY] > lock_limit:
                msg = f"The lock was broken. Previous lock id: {result[self.LOCK_KEY]}"
                warnings.warn(msg)

            self.locked_document = result

    def release(self, exc_type, exc_val, exc_tb):
        # Release the lock by removing the unique identifier and lock expiration time
        update = {"$unset": {self.LOCK_KEY: "", self.LOCK_TIME_KEY: ""}}
        # TODO maybe set on release only if not exception was raised?
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
            warnings.warn(msg)

        self.locked_document = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):

        if self.locked_document:
            self.release(exc_type, exc_val, exc_tb)
