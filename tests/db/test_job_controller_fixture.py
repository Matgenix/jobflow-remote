"""Tests for the snapshot based restore of the DB done by the job_controller fixture."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from jobflow_remote.jobs.jobcontroller import JobController


def _db_state(jc: JobController) -> dict[str, Any]:
    """
    Documents (without ``_id``) and indexes of the collections handled by reset.

    Parameters
    ----------
    jc
        The JobController to inspect.

    Returns
    -------
    dict
        The documents and indexes of each collection, by collection name.
    """
    from tests.conftest import _tracked_collections

    state: dict[str, Any] = {}
    for name, coll in _tracked_collections(jc).items():
        docs = [{k: v for k, v in d.items() if k != "_id"} for d in coll.find()]
        # jobstore collections are not cleaned by the reset
        if name.startswith("jobstore_"):
            docs = []
        state[name] = {"docs": docs, "indexes": coll.index_information()}
    return state


def test_restore_matches_reset(job_controller_reset, one_job) -> None:
    from tests.conftest import _prepare_db

    jc = job_controller_reset
    assert jc.count_jobs() == 1

    # the restored state should be the same as after a full reset
    _prepare_db(jc, full_reset=False)
    restored = _db_state(jc)
    assert jc.count_jobs() == 0
    assert jc.count_flows() == 0

    assert jc.reset(max_limit=0)
    assert restored == _db_state(jc)


def test_restore_after_index_change(job_controller_reset) -> None:
    from tests.conftest import _prepare_db

    jc = job_controller_reset
    expected = _db_state(jc)

    # a test that modifies the indexes should lead to a full reset
    jc.jobs.create_index("job.metadata.test_custom_index")
    jc.flows.drop_indexes()
    assert _db_state(jc) != expected
    _prepare_db(jc, full_reset=False)
    assert _db_state(jc) == expected


def test_restore_auxiliary(job_controller_reset) -> None:
    from tests.conftest import _prepare_db

    jc = job_controller_reset
    expected = _db_state(jc)

    jc.auxiliary.update_one({"next_id": {"$exists": True}}, {"$set": {"next_id": 42}})
    jc.auxiliary.insert_one({"extra": 1})
    jc.batches.insert_one({"batch_uid": "test"})
    _prepare_db(jc, full_reset=False)
    assert _db_state(jc) == expected
