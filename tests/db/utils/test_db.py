import threading
import time
from contextlib import ExitStack

import pytest

from jobflow_remote.utils.db import MongoLock


@pytest.fixture()
def test_collection(mongoclient, store_database_name):
    collection = mongoclient[store_database_name]["test_lock_collection"]
    collection.delete_many({})
    collection.insert_many([{"test_id": 1}, {"test_id": 2}])
    yield collection
    collection.delete_many({})


def test_mongo_lock_acquire(test_collection):
    with MongoLock(test_collection, {"test_id": 1}) as lock:
        assert lock.locked_document is not None
        assert lock.locked_document["test_id"] == 1
        assert (
            test_collection.count_documents(
                {"test_id": 1, MongoLock.LOCK_KEY: lock.lock_id}
            )
            == 1
        )
        assert (
            test_collection.count_documents({"test_id": 2, MongoLock.LOCK_KEY: None})
            == 1
        )

    assert (
        test_collection.count_documents({"test_id": 1, MongoLock.LOCK_KEY: None}) == 1
    )


def test_mongo_lock_acquire_already_locked(test_collection):
    with MongoLock(test_collection, {"test_id": 1}) as lock:
        assert lock.locked_document is not None
        with MongoLock(test_collection, {"test_id": 1}) as another_lock:
            assert another_lock.locked_document is None
            assert another_lock.unavailable_document is None

        # check with get_locked_doc
        with MongoLock(
            test_collection, {"test_id": 1}, get_locked_doc=True
        ) as another_lock:
            assert another_lock.locked_document is None
            assert another_lock.unavailable_document is not None


def lock_and_sleep(test_collection, thread_lock):
    with MongoLock(test_collection, {"test_id": 1}, sleep=0.1) as lock:
        assert lock.locked_document is not None
        thread_lock.set()
        time.sleep(2)


def test_mongo_lock_max_wait(test_collection):
    thread_lock = threading.Event()

    t = threading.Thread(target=lock_and_sleep, args=(test_collection, thread_lock))
    t.start()

    thread_lock.wait()
    with MongoLock(test_collection, {"test_id": 1}, sleep=0.1, max_wait=0.1) as lock:
        assert lock.locked_document is None

    with MongoLock(test_collection, {"test_id": 1}, sleep=0.5, max_wait=10) as lock:
        assert lock.locked_document is not None

    t.join()


def test_mongo_lock_break_lock(test_collection):
    # the warning is raised by the first lock. It will not be able to release the lock
    # since it was already released by the second lock.
    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        MongoLock(test_collection, {"test_id": 1}) as lock,
    ):
        assert (
            test_collection.count_documents(
                {"test_id": 1, MongoLock.LOCK_KEY: lock.lock_id}
            )
            == 1
        )
        with MongoLock(
            test_collection, {"test_id": 1}, break_lock=True
        ) as another_lock:
            assert another_lock.locked_document is not None
            assert (
                test_collection.count_documents(
                    {"test_id": 1, MongoLock.LOCK_KEY: another_lock.lock_id}
                )
                == 1
            )


def test_mongo_lock_update_on_release(test_collection):
    with MongoLock(test_collection, {"test_id": 1}) as lock:
        lock.update_on_release = {"$set": {"status": "updated"}}
        assert test_collection.count_documents({"test_id": 1, "status": "updated"}) == 0
    assert test_collection.count_documents({"test_id": 1, "status": "updated"}) == 1


def test_mongo_lock_update_on_release_multiple_fields(test_collection):
    with MongoLock(test_collection, {"test_id": 1}) as lock:
        lock.update_on_release = {"$set": {"status": "updated", "counter": 1}}
        assert (
            test_collection.count_documents(
                {"test_id": 1, "status": "updated", "counter": 1}
            )
            == 0
        )
    assert (
        test_collection.count_documents(
            {"test_id": 1, "status": "updated", "counter": 1}
        )
        == 1
    )


def test_mongo_lock_update_on_release_with_exception(test_collection):
    # mypy does not like multiple statements inside pytest.raises inside, but
    # they are necessary to properly test the functionality. This is the only
    # way I managed to make it accept it.
    with ExitStack() as stack:
        stack.enter_context(pytest.raises(RuntimeError, match="Test exception"))
        lock = stack.enter_context(MongoLock(test_collection, {"test_id": 1}))
        lock.update_on_release = {"$set": {"status": "updated"}}
        raise RuntimeError("Test exception")

    assert test_collection.count_documents({"test_id": 1, "status": "updated"}) == 0


def test_mongo_lock_delete_on_release(test_collection):
    with MongoLock(test_collection, {"test_id": 1}) as lock:
        lock.delete_on_release = True
        assert test_collection.count_documents({"test_id": 1}) == 1
    assert test_collection.count_documents({"test_id": 1}) == 0


def test_mongo_lock_delete_on_release_with_exception(test_collection):
    # mypy does not like multiple statements inside pytest.raises inside, but
    # they are necessary to properly test the functionality. This is the only
    # way I managed to make it accept it.
    with ExitStack() as stack:
        stack.enter_context(pytest.raises(RuntimeError, match="Test exception"))
        lock = stack.enter_context(MongoLock(test_collection, {"test_id": 1}))
        lock.delete_on_release = True
        raise RuntimeError("Test exception")

    assert test_collection.count_documents({"test_id": 1}) == 1


class TestMongoLock:
    def test_context(self, mongoclient):
        db = mongoclient["mytestdb"]
        collection = db["mytestcollection"]
        collection.drop()
        collection.insert_one({"a": 1})
        collection.insert_one({"b": 2})
        with MongoLock(collection=collection, filter={"a": 1}) as lock:
            assert lock.locked_document is not None
            lock_id = lock.lock_id
            assert lock_id == lock.get_lock_id(lock.locked_document)
            assert lock.is_locked is False
            with MongoLock(collection=collection, filter={"a": 1}) as lock2:
                assert lock2.locked_document is None
                assert lock2.unavailable_document is None
                assert lock2.is_locked is True
            with MongoLock(
                collection=collection, filter={"a": 1}, get_locked_doc=True
            ) as lock3:
                assert lock3.locked_document is None
                assert lock3.unavailable_document is not None
                assert lock_id == lock3.get_lock_id(lock3.unavailable_document)
                assert lock3.is_locked is True
