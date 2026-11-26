import os.path
from shutil import which
from typing import NoReturn

import pymongo
import pytest


def test_submit_flow(job_controller, runner) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_second = add(add_first.output, 5)

    flow = Flow([add_first, add_second])
    submit_flow(flow, worker="test_local_worker")

    runner.run_all_jobs(max_seconds=10)

    assert len(job_controller.get_jobs({})) == 2
    job_1, job_2 = job_controller.get_jobs({})
    assert job_1["job"]["function_args"] == [1, 5]
    assert job_1["job"]["name"] == "add"

    output_1 = job_controller.jobstore.get_output(uuid=job_1["uuid"])
    assert output_1 == 6
    output_2 = job_controller.jobstore.get_output(uuid=job_2["uuid"])
    assert output_2 == 11
    assert (
        job_controller.count_jobs(states=JobState.COMPLETED) == 2
    ), f"Jobs not marked as completed, full job info:\n{job_controller.get_jobs({})}"
    assert (
        job_controller.count_flows(states=FlowState.COMPLETED) == 1
    ), f"Flows not marked as completed, full flow info:\n{job_controller.get_flows({})}"


def test_queries(job_controller, runner) -> None:
    """Test different options to query Jobs and Flows."""
    import datetime

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    add_first = add(1, 5)
    add_first.name = "add_job1"
    add_second = add(add_first.output, 5)
    add_second.name = "add_job2"

    add_first.update_metadata({"test_meta": 1})

    flow = Flow([add_first, add_second])
    flow.name = "flow_1"
    submit_flow(flow, worker="test_local_worker")

    add_third = add(1, 5)
    add_third.name = "add_job3"
    add_fourth = add(add_third.output, 5)
    add_fourth.name = "add_job4"

    flow2 = Flow([add_third, add_fourth])
    flow2.name = "flow_2"
    submit_flow(flow2, worker="test_local_worker")

    date_create = datetime.datetime.now()

    assert runner.run_one_job(max_seconds=10)

    assert job_controller.count_jobs(states=JobState.COMPLETED) == 1
    assert job_controller.count_jobs(states=JobState.READY) == 2
    assert job_controller.count_jobs(db_ids="1") == 1
    assert job_controller.count_jobs(job_ids=(add_first.uuid, 1)) == 1
    # capped and estimated counts
    assert job_controller.count_jobs(limit=2) == 2
    assert job_controller.count_jobs(estimated=True) == 4
    assert job_controller.count_jobs(states=JobState.READY, estimated=True) == 2
    assert job_controller.count_flows(estimated=True, limit=1) == 1
    jobs_start_date = job_controller.get_jobs_info(start_date=date_create)
    assert len(jobs_start_date) == 2
    assert {j.uuid for j in jobs_start_date} == {add_first.uuid, add_second.uuid}

    jobs_end_date = job_controller.get_jobs_info(end_date=date_create)
    assert {j.uuid for j in jobs_end_date} == {add_third.uuid, add_fourth.uuid}

    assert job_controller.count_jobs(metadata={"test_meta": 1}) == 1

    assert job_controller.count_jobs(name="add_job") == 0
    assert job_controller.count_jobs(name="add_job1") == 1
    assert job_controller.count_jobs(name="add_job*") == 4

    assert job_controller.count_jobs(flow_ids=flow.uuid) == 2

    assert job_controller.count_jobs(workers="test_local_worker") == 4
    assert job_controller.count_jobs(workers="xxx") == 0

    with job_controller.lock_job(filter={"uuid": add_second.uuid}):
        assert job_controller.count_jobs(locked=True) == 1

    assert (
        job_controller.count_jobs(
            query={"uuid": {"$in": (add_first.uuid, add_second.uuid)}}
        )
        == 2
    )

    # test skip
    skipped_jobs = job_controller.get_jobs_info(sort=["db_id"], skip=1)
    assert len(skipped_jobs) == job_controller.count_jobs() - 1
    assert skipped_jobs[0].db_id == "2"

    assert job_controller.count_flows(states=FlowState.READY) == 1
    assert job_controller.count_flows(states=FlowState.RUNNING) == 1
    assert job_controller.count_flows(job_ids=add_first.uuid) == 1
    assert job_controller.count_flows(db_ids="1") == 1
    assert job_controller.count_flows(flow_ids=flow.uuid) == 1
    assert job_controller.count_flows(start_date=date_create) == 1
    assert job_controller.count_flows(end_date=date_create) == 1
    assert job_controller.count_flows(name="flow_1") == 1
    assert job_controller.count_flows(name="flow_*") == 2
    assert (
        job_controller.count_flows(query={"uuid": {"$in": (flow.uuid, flow2.uuid)}})
        == 2
    )

    # test skip
    skipped_flows = job_controller.get_flows_info(sort=["updated_on"], skip=1)
    assert len(skipped_flows) == job_controller.count_flows() - 1
    assert (
        skipped_flows[0].updated_on
        == job_controller.get_flows_info(sort=["updated_on"])[1].updated_on
    )


def test_get_jobs_db_ids(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.jobs.state import JobState

    all_docs = job_controller.get_jobs_doc()
    all_db_ids = [doc.db_id for doc in all_docs]
    assert len(all_db_ids) == 4

    # no filter: all the db_ids are returned, as strings
    res = job_controller.get_jobs_db_ids()
    assert all(isinstance(db_id, str) for db_id in res)
    assert sorted(res) == sorted(all_db_ids)

    # filter by name
    doc_job1 = next(doc for doc in all_docs if doc.job.name == "add_job1")
    assert job_controller.get_jobs_db_ids(name="add_job1") == [doc_job1.db_id]

    # filter by flow id
    flow_1_uuids = {job.uuid for job in two_flows_four_jobs[0]}
    flow_1_db_ids = [doc.db_id for doc in all_docs if doc.uuid in flow_1_uuids]
    assert len(flow_1_db_ids) == 2
    res = job_controller.get_jobs_db_ids(flow_ids=two_flows_four_jobs[0].uuid)
    assert sorted(res) == sorted(flow_1_db_ids)

    # filter by state: only the first job of each flow is READY
    ready_db_ids = [doc.db_id for doc in all_docs if doc.state == JobState.READY]
    assert len(ready_db_ids) == 2
    res = job_controller.get_jobs_db_ids(states=JobState.READY)
    assert sorted(res) == sorted(ready_db_ids)

    # combined filters
    res = job_controller.get_jobs_db_ids(
        flow_ids=two_flows_four_jobs[0].uuid, states=JobState.READY
    )
    assert res == [doc_job1.db_id]

    # sort and limit
    res = job_controller.get_jobs_db_ids(sort=[("db_id", -1)], limit=1)
    assert len(res) == 1

    # pages with skip and limit: not overlapping and in the sort order
    sort = [("db_id", 1)]
    all_sorted = job_controller.get_jobs_db_ids(sort=sort)
    page_1 = job_controller.get_jobs_db_ids(sort=sort, limit=2)
    page_2 = job_controller.get_jobs_db_ids(sort=sort, limit=2, skip=2)
    assert page_1 == all_sorted[:2]
    assert page_2 == all_sorted[2:]
    assert job_controller.get_jobs_db_ids(sort=sort, skip=4) == []

    # no match
    assert job_controller.get_jobs_db_ids(name="missing_job") == []
    assert job_controller.get_jobs_db_ids(custom_query={"db_id": "missing"}) == []


def test_get_flows_uuids(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.jobs.state import FlowState

    flow_1, flow_2 = two_flows_four_jobs

    # no filter: all the flow uuids are returned
    res = job_controller.get_flows_uuids()
    assert sorted(res) == sorted([flow_1.uuid, flow_2.uuid])

    # filter by name
    assert job_controller.get_flows_uuids(name="flow_1") == [flow_1.uuid]

    # filter by metadata
    assert job_controller.get_flows_uuids(metadata={"f1_metadata": "some_info"}) == [
        flow_1.uuid
    ]

    # filter by flow id
    assert job_controller.get_flows_uuids(flow_ids=flow_2.uuid) == [flow_2.uuid]

    # filter by uuid of a job belonging to the flow
    assert job_controller.get_flows_uuids(job_ids=flow_2[0].uuid) == [flow_2.uuid]

    # filter by db_id of a job belonging to the flow
    job_doc = job_controller.get_job_doc(job_id=flow_1[0].uuid)
    assert job_controller.get_flows_uuids(db_ids=job_doc.db_id) == [flow_1.uuid]

    # filter by state
    res = job_controller.get_flows_uuids(states=FlowState.READY)
    assert sorted(res) == sorted([flow_1.uuid, flow_2.uuid])
    assert job_controller.get_flows_uuids(states=FlowState.COMPLETED) == []

    # sort and limit
    res = job_controller.get_flows_uuids(sort=[("name", 1)], limit=1)
    assert res == [flow_1.uuid]
    res = job_controller.get_flows_uuids(sort=[("name", -1)], limit=1)
    assert res == [flow_2.uuid]
    res = job_controller.get_flows_uuids(sort=[("name", -1)])
    assert res == [flow_2.uuid, flow_1.uuid]

    # pages with skip and limit
    assert job_controller.get_flows_uuids(sort=[("name", 1)], limit=1, skip=1) == [
        flow_2.uuid
    ]
    assert job_controller.get_flows_uuids(sort=[("name", -1)], skip=1) == [flow_1.uuid]
    assert job_controller.get_flows_uuids(sort=[("name", 1)], skip=2) == []

    # no match
    assert job_controller.get_flows_uuids(name="missing_flow") == []


def test_query_flow_ids_no_match(job_controller, two_flows_four_jobs) -> None:
    flow_1, _ = two_flows_four_jobs
    flow_1_job_uuids = sorted(job.uuid for job in flow_1)

    # a value that is not a uuid is a Job db_id. If no Flow contains that
    # Job, nothing should match.
    assert job_controller.count_jobs(flow_ids="no-such-id") == 0
    assert job_controller.get_jobs_info(flow_ids="no-such-id") == []
    assert job_controller.get_jobs_db_ids(flow_ids="no-such-id") == []

    # mixing a valid Flow uuid and an unknown value only returns that Flow's Jobs
    jobs_info = job_controller.get_jobs_info(flow_ids=[flow_1.uuid, "no-such-id"])
    assert sorted(ji.uuid for ji in jobs_info) == flow_1_job_uuids
    assert job_controller.count_jobs(flow_ids=[flow_1.uuid, "no-such-id"]) == 2

    # the db_id of a Job is resolved to the Flow containing it
    db_id = job_controller.get_job_doc(job_id=flow_1[0].uuid).db_id
    jobs_info = job_controller.get_jobs_info(flow_ids=[db_id, "no-such-id"])
    assert sorted(ji.uuid for ji in jobs_info) == flow_1_job_uuids


def test_flows_custom_query(job_controller, two_flows_four_jobs) -> None:
    from jobflow_remote.jobs.state import FlowState

    flow_1, flow_2 = two_flows_four_jobs

    def check(expected: list[str], **kwargs) -> None:
        custom_query = kwargs.pop("custom_query")
        infos = job_controller.get_flows_info(custom_query=custom_query, **kwargs)
        assert sorted(fi.flow_id for fi in infos) == sorted(expected)
        infos = job_controller.get_flows_info(
            custom_query=custom_query, with_jobs_info=True, **kwargs
        )
        assert sorted(fi.flow_id for fi in infos) == sorted(expected)
        uuids = job_controller.get_flows_uuids(custom_query=custom_query, **kwargs)
        assert sorted(uuids) == sorted(expected)
        assert job_controller.count_flows(query=custom_query, **kwargs) == len(expected)

    # alone
    check([flow_1.uuid], custom_query={"name": "flow_1"})
    check(
        [flow_1.uuid, flow_2.uuid],
        custom_query={"name": {"$in": ["flow_1", "flow_2"]}},
    )
    check([], custom_query={"name": "missing"})

    # combined with the other filters, not replaced by them
    check(
        [flow_1.uuid],
        custom_query={"metadata.f1_metadata": "some_info"},
        states=FlowState.READY,
    )
    check([], custom_query={"name": "flow_1"}, flow_ids=flow_2.uuid)
    check([flow_2.uuid], custom_query={"uuid": {"$ne": flow_1.uuid}}, name="flow_*")

    # keys overlapping with the other filters
    with pytest.raises(ValueError, match="must not overlap"):
        job_controller.get_flows_info(custom_query={"name": "flow_1"}, name="flow_1")
    with pytest.raises(ValueError, match="must not overlap"):
        job_controller.get_flows_uuids(
            custom_query={"uuid": flow_1.uuid}, flow_ids=flow_1.uuid
        )
    with pytest.raises(ValueError, match="must not overlap"):
        job_controller.count_flows(query={"state": "READY"}, states=FlowState.READY)


NAMES_FILTER = [
    "relax",
    "relax_1",
    "relax_2",
    "relaxation",
    "pre_relax",
    "relax.x-1",
    "relax.x-2",
    "relaxyx-1",
    "rel+ax",
    "re(l)ax",
    "rel-ax",
    "relbx",
    "rel\\ax",
    "relax\\b",
    "static",
]


@pytest.fixture()
def flows_with_names(random_project_name):
    """Add one flow with one job for each name. Flow and Job share the name."""
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add

    for name in NAMES_FILTER:
        job = add(1, 2)
        job.name = name
        flow = Flow([job], name=name)
        submit_flow(flow, worker="test_local_worker")


NAME_PATTERNS = [
    "relax",
    "relax*",
    "*relax*",
    "*relax",
    "rel?x",
    "rel[ab]x",
    "relax.x*",
    "relax.x-*",
    "relax.*",
    "rel+ax",
    "re(l)ax",
    "re(l*",
    "rel-ax",
    "r*x*1",
    "rel\\ax",
    "relax\\b",
    "rel\\*",
    "*\\*",
    "missing",
]


def test_name_filter(job_controller, flows_with_names) -> None:
    """
    Check the name filter for all the patterns on the same set of Flows.

    The patterns are checked in a loop, instead of parametrizing the test, to
    avoid populating the DB for each of them. All the failures are collected
    and reported together.
    """
    import fnmatch

    failures: list[str] = []

    def check(description: str, actual: object, expected: object) -> None:
        if actual != expected:
            failures.append(f"{description}: got {actual!r}, expected {expected!r}")

    for pattern in NAME_PATTERNS:
        expected = sorted(n for n in NAMES_FILTER if fnmatch.fnmatchcase(n, pattern))
        # the full translation of the pattern, without the index optimizations
        full_regex = {"$regex": "^" + fnmatch.translate(pattern)}

        # jobs
        check(
            f"[{pattern}] count_jobs",
            job_controller.count_jobs(name=pattern),
            len(expected),
        )
        check(
            f"[{pattern}] jobs full regex",
            job_controller.jobs.count_documents({"job.name": full_regex}),
            len(expected),
        )
        jobs_info = job_controller.get_jobs_info(name=pattern)
        check(
            f"[{pattern}] get_jobs_info", sorted(ji.name for ji in jobs_info), expected
        )

        # flows
        check(
            f"[{pattern}] count_flows",
            job_controller.count_flows(name=pattern),
            len(expected),
        )
        check(
            f"[{pattern}] flows full regex",
            job_controller.flows.count_documents({"name": full_regex}),
            len(expected),
        )
        flows_info = job_controller.get_flows_info(name=pattern)
        check(
            f"[{pattern}] get_flows_info",
            sorted(fi.name for fi in flows_info),
            expected,
        )

    assert not failures, "\n".join(failures)


def test_get_flows_info_with_jobs_info_pages(
    job_controller, flows_with_names, mocker
) -> None:
    sort = [("name", 1)]
    pages = []
    for skip in (0, 3):
        spy = mocker.spy(job_controller.flows, "aggregate")
        flows_info = job_controller.get_flows_info(
            sort=sort, limit=3, skip=skip, with_jobs_info=True
        )
        # the jobs are fetched with a separate query, not with an aggregation
        assert spy.call_count == 0
        mocker.stop(spy)

        assert len(flows_info) == 3
        assert all(len(fi.jobs_info) == 1 for fi in flows_info)
        assert all(fi.jobs_info[0].name == fi.name for fi in flows_info)
        ids = [fi.flow_id for fi in flows_info]
        # same page as without the jobs info
        ids_no_jobs = [
            fi.flow_id
            for fi in job_controller.get_flows_info(sort=sort, limit=3, skip=skip)
        ]
        assert ids == ids_no_jobs
        pages.append(ids)

    assert set(pages[0]).isdisjoint(pages[1])
    expected_names = sorted(NAMES_FILTER)[3:6]
    assert [
        fi.name
        for fi in job_controller.get_flows_info(
            sort=sort, limit=3, skip=3, with_jobs_info=True
        )
    ] == expected_names

    # with a filter as well
    flows_info = job_controller.get_flows_info(
        name="relax*", sort=sort, limit=2, skip=1, with_jobs_info=True
    )
    assert [fi.name for fi in flows_info] == sorted(
        n for n in NAMES_FILTER if n.startswith("relax")
    )[1:3]
    assert all(fi.jobs_info[0].name == fi.name for fi in flows_info)


def test_get_flows_info_with_jobs_info(
    job_controller, two_flows_four_jobs, mocker
) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.data import FlowDoc, FlowInfo, projection_flow_info_jobs
    from jobflow_remote.testing import add

    # a nested Flow: the hosts of the inner Job include both Flows
    inner_job = add(1, 2)
    inner_job.name = "inner_job"
    outer_job = add(inner_job.output, 3)
    outer_job.name = "outer_job"
    nested_flow = Flow([Flow([inner_job], name="inner"), outer_job], name="nested")
    submit_flow(nested_flow, worker="test_local_worker")

    flow_1, flow_2 = two_flows_four_jobs
    expected_jobs = {
        flow_1.uuid: ["add_job1", "add_job2"],
        flow_2.uuid: ["add_job3", "add_job4"],
        nested_flow.uuid: ["inner_job", "outer_job"],
    }

    sort = [("created_on", 1)]
    jobs_sort = [("job.name", -1)]
    flows_info = job_controller.get_flows_info(
        sort=sort, jobs_sort=jobs_sort, with_jobs_info=True
    )
    assert [fi.flow_id for fi in flows_info] == list(expected_jobs)
    for fi in flows_info:
        # each Flow has its own Jobs, in the order of jobs_sort
        names = [ji.name for ji in fi.jobs_info]
        assert names == sorted(expected_jobs[fi.flow_id], reverse=True)
        assert fi.job_names == names
        assert [ji.db_id for ji in fi.jobs_info] == fi.db_ids

    # same output as the aggregation
    data = job_controller.get_flow_job_aggreg(
        sort=sort,
        jobs_sort=jobs_sort,
        projection_flow={k: 1 for k in FlowDoc.model_fields},
        projection_job={f: 1 for f in projection_flow_info_jobs},
    )
    assert [FlowInfo.from_query_dict(d) for d in data] == flows_info

    # additional Job properties
    flows_info = job_controller.get_flows_info(
        flow_ids=flow_1.uuid, with_jobs_info=["remote"]
    )
    assert len(flows_info) == 1
    assert len(flows_info[0].jobs_info) == 2

    # a Flow without matching Jobs
    assert (
        job_controller.get_flows_info(flow_ids="no-such-id", with_jobs_info=True) == []
    )

    # sorting on the fields of the Jobs requires the aggregation
    spy = mocker.spy(job_controller.flows, "aggregate")
    flows_info = job_controller.get_flows_info(
        sort=[("jobs_list.job.name", 1)], with_jobs_info=True
    )
    assert spy.call_count == 1
    assert [fi.flow_id for fi in flows_info] == list(expected_jobs)


def test_flows_info_lock(job_controller, two_flows_four_jobs) -> None:
    flow_1, flow_2 = two_flows_four_jobs

    # the three ways of reading the Flows: the Flow documents only, with the
    # Jobs from a separate query and with the aggregation (sort on the Jobs)
    read_options = [
        {},
        {"with_jobs_info": True},
        {"with_jobs_info": True, "sort": [("jobs_list.db_id", 1)]},
    ]

    for kwargs in read_options:
        for fi in job_controller.get_flows_info(**kwargs):
            assert fi.lock_id is None
            assert fi.lock_time is None
            assert not fi.is_locked

    with job_controller.lock_flow(filter={"uuid": flow_1.uuid}):
        flow_doc = job_controller.flows.find_one({"uuid": flow_1.uuid})
        assert flow_doc["lock_id"] is not None
        for kwargs in read_options:
            infos = {fi.flow_id: fi for fi in job_controller.get_flows_info(**kwargs)}
            assert infos[flow_1.uuid].is_locked
            assert infos[flow_1.uuid].lock_id == flow_doc["lock_id"]
            assert infos[flow_1.uuid].lock_time == flow_doc["lock_time"]
            assert not infos[flow_2.uuid].is_locked
            assert infos[flow_2.uuid].lock_time is None

        locked = job_controller.get_flows_info(locked=True)
        assert [fi.flow_id for fi in locked] == [flow_1.uuid]

    assert not job_controller.get_flows_info(flow_ids=flow_1.uuid)[0].is_locked


def test_get_flow_job_aggreg_jobs_list_query(
    job_controller, flows_with_names, mocker
) -> None:
    # a query and a sort referring to the jobs need to be applied after the $lookup
    spy = mocker.spy(job_controller.flows, "aggregate")
    data = job_controller.get_flow_job_aggreg(
        query={"jobs_list.job.name": {"$in": ["relax", "static", "relbx"]}},
        sort=[("jobs_list.job.name", -1)],
        skip=1,
        limit=1,
    )
    stages = [next(iter(stage)) for stage in spy.call_args.args[0]]
    assert stages == ["$lookup", "$match", "$sort", "$skip", "$limit"]
    assert [d["name"] for d in data] == ["relbx"]


def test_rerun_completed(job_controller, runner) -> None:
    from pathlib import Path

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add
    from jobflow_remote.utils.db import JobLockedError

    j1 = add(1, 2)
    j2 = add(j1.output, 2)
    j3 = add(j2.output, 2)
    flow = Flow([j1, j2, j3])

    submit_flow(flow, worker="test_local_worker")

    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])
    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j2_info = job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index)
    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)
    j1_path = Path(j1_info.run_dir)
    assert j1_info.state == JobState.COMPLETED
    assert j2_info.state == JobState.READY
    assert j3_info.state == JobState.WAITING

    # try rerunning the second and third job. Wrong state
    with pytest.raises(ValueError, match="The Job is in the READY state"):
        job_controller.rerun_job(job_id=j2.uuid, job_index=j2.index)

    with pytest.raises(ValueError, match="The Job is in the WAITING state"):
        job_controller.rerun_job(job_id=j3.uuid, job_index=j3.index)

    # check that the state did not change
    j2_info = job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index)
    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)

    assert j2_info.state == JobState.READY
    assert j3_info.state == JobState.WAITING

    assert len(list(j1_path.iterdir())) > 0

    # since the first job is completed, the force option is required
    with pytest.raises(ValueError, match="Job in state COMPLETED cannot be rerun"):
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index)

    # two jobs are modified, as j2 is switched to WAITING
    # Use rerun_jobs instead of rerun_job to test that as well
    assert set(job_controller.rerun_jobs(job_ids=(j1.uuid, j1.index), force=True)) == {
        j1_info.db_id,
        j2_info.db_id,
    }

    # the folder should still exist
    # create a file there so that it will be checked after the execution. It should
    # be deleted by the runner.
    assert j1_path.exists()
    check_file_j1 = j1_path / "test_test.json"
    check_file_j1.touch(exist_ok=True)
    assert check_file_j1.exists()

    assert (
        job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index).state
        == JobState.READY
    )
    assert (
        job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index).state
        == JobState.WAITING
    )
    assert job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.READY

    # run all the jobs
    runner.run_all_jobs(max_seconds=20)
    assert job_controller.count_jobs(states=JobState.COMPLETED) == 3
    with pytest.raises(ValueError, match="Job in state COMPLETED cannot be rerun"):
        job_controller.rerun_job(job_id=j3.uuid, job_index=j3.index)

    assert not check_file_j1.exists()

    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)
    j3_path = Path(j3_info.run_dir)

    assert len(list(j3_path.iterdir())) > 0

    # The last job can be rerun, but still needs the "force" option.
    # Don't delete files
    assert set(
        job_controller.rerun_job(
            job_id=j3.uuid, job_index=j3.index, force=True, delete_files=False
        )
    ) == {j3_info.db_id}

    assert len(list(j3_path.iterdir())) > 0

    # The remaining tests are to verify that everything is correct with locked jobs
    # as well
    with job_controller.lock_job(filter={"uuid": j2.uuid}):
        with pytest.raises(
            JobLockedError,
            match=f"Job with db_id {j2_info.db_id} is locked with lock_id",
        ):
            job_controller.rerun_job(job_id=j2.uuid, job_index=j2.index)

        # try waiting, but fails in this case as well
        with pytest.raises(
            JobLockedError,
            match=f"Job with db_id {j2_info.db_id} is locked with lock_id",
        ):
            job_controller.rerun_job(job_id=j2.uuid, job_index=j2.index, wait=1)

    # The rerun fails even if a child is locked
    with (
        job_controller.lock_job(filter={"uuid": j3.uuid}),
        pytest.raises(
            JobLockedError,
            match=f"Job with db_id {j3_info.db_id} is locked with lock_id",
        ),
    ):
        job_controller.rerun_job(job_id=j2.uuid, job_index=j2.index, force=True)

    assert (
        job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index).state
        == JobState.COMPLETED
    )

    assert (
        job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.RUNNING
    )

    # can rerun if breaking the lock
    # catch the warning coming from MongoLock
    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        job_controller.lock_job(filter={"uuid": j2.uuid}),
    ):
        assert set(
            job_controller.rerun_job(
                job_id=j2.uuid, job_index=j2.index, force=True, break_lock=True
            )
        ) == {j2_info.db_id, j3_info.db_id}

    assert (
        job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index).state
        == JobState.READY
    )


def test_rerun_failed(job_controller, runner) -> None:
    from pathlib import Path

    from jobflow import Flow, OnMissing

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add, always_fails, ignore_input, self_replace

    j1 = always_fails()
    j2 = add(j1.output, 2)
    j3 = ignore_input(j1.output)
    j3.config.on_missing_references = OnMissing.NONE
    j4 = self_replace(j3.output)
    flow = Flow([j1, j2, j3, j4])

    submit_flow(flow, worker="test_local_worker")

    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])

    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j2_info = job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index)
    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)
    j4_info = job_controller.get_job_info(job_id=j4.uuid, job_index=j4.index)

    assert j1_info.state == JobState.FAILED
    assert j2_info.state == JobState.WAITING
    assert j3_info.state == JobState.READY
    assert j4_info.state == JobState.WAITING

    assert job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.FAILED

    # rerun without "force". Since the job is FAILED and the children are
    # WAITING or READY
    assert set(job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index)) == {
        j1_info.db_id,
        j3_info.db_id,
    }

    assert job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.READY

    assert job_controller.count_jobs(states=JobState.READY) == 1

    # run the first job again and the job with OnMissing.None
    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])
    assert runner.run_one_job(max_seconds=10, job_id=[j3.uuid, j3.index])

    # cannot rerun the first as one of the children is COMPLETED
    with pytest.raises(
        ValueError,
        match="The child of Job.*has state COMPLETED which is not acceptable",
    ):
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index)

    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)
    j1_path = Path(j1_info.run_dir)
    j3_path = Path(j3_info.run_dir)

    assert len(list(j1_path.iterdir())) > 0
    assert len(list(j3_path.iterdir())) > 0

    # can be rerun with the "force" option
    assert set(
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index, force=True)
    ) == {j1_info.db_id, j3_info.db_id, j4_info.db_id}

    assert job_controller.count_jobs(states=JobState.READY) == 1

    # check that also the cleanup of the child was set
    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)
    assert j1_path.exists()
    assert j3_path.exists()
    assert j1_info.remote.prerun_cleanup
    assert j3_info.remote.prerun_cleanup

    # create a file in the folders, to be sure that in the meanwhile the folders have been removed
    # during the upload phase
    check_file_j1 = j1_path / "test_test.json"
    check_file_j1.touch(exist_ok=True)
    assert check_file_j1.exists()
    check_file_j3 = j3_path / "test_test.json"
    check_file_j3.touch(exist_ok=True)
    assert check_file_j3.exists()

    # run again the jobs with j4. This generates a replace
    # check that the fake additional files are not present, meaning the
    # folder was deleted by the cleanup
    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])
    assert not check_file_j1.exists()
    assert runner.run_one_job(max_seconds=10, job_id=[j3.uuid, j3.index])
    assert not check_file_j3.exists()
    assert runner.run_one_job(max_seconds=10, job_id=[j4.uuid, j4.index])

    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)

    # also check that the cleanup has be set to False
    assert not j1_info.remote.prerun_cleanup
    assert not j3_info.remote.prerun_cleanup

    assert job_controller.count_jobs(job_ids=(j4.uuid, 2)) == 1

    # At this point it is impossible to rerun, even with the "force" option
    # because it will require rerunning j4, which is COMPLETED with a replacement
    # already existing in the DB.
    with pytest.raises(
        ValueError,
        match="The child of Job.*has state COMPLETED which is not acceptable",
    ):
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index)

    with pytest.raises(
        ValueError, match="Job.*has a child job.*which is not the last index"
    ):
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index, force=True)

    assert job_controller.get_job_info(db_id=j1_info.db_id).state == JobState.FAILED
    assert (
        job_controller.get_job_info(job_id=j4.uuid, job_index=2).state == JobState.READY
    )

    assert job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.FAILED


def test_rerun_remote_error(job_controller, monkeypatch, runner) -> None:
    from pathlib import Path

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    j2 = add(j1.output, 2)
    flow = Flow([j1, j2])

    submit_flow(flow, worker="test_local_worker")

    # patch the upload method of the runner to trigger a remote error
    def submit_error(self, lock) -> NoReturn:
        raise RuntimeError("FAKE ERROR")

    with monkeypatch.context() as m:
        m.setattr(Runner, "submit", submit_error)
        # patch this to 1 to avoid retrying multiple times
        m.setattr(runner.runner_options, "max_step_attempts", 1)
        with pytest.warns(match="FAKE ERROR"):
            assert runner.run_one_job(max_seconds=10, job_id=(j1.uuid, j1.index))

    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    assert j1_info.state == JobState.REMOTE_ERROR
    assert (
        job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.RUNNING
    )

    assert len(list(Path(j1_info.run_dir).iterdir())) > 0

    # can rerun without "force"
    assert job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index, force=True) == [
        j1_info.db_id
    ]
    assert (
        job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index).state
        == JobState.READY
    )
    assert job_controller.get_flows_info(job_ids=[j1.uuid])[0].state == FlowState.READY

    # check that files are marked for deletion also in case of remote_error.
    # do not run the job explicitly, as it is already checked in other tests
    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    assert Path(j1_info.run_dir).exists()
    assert j1_info.remote.prerun_cleanup


def test_rerun_generic(job_controller):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    j2 = add(j1.output, 2)
    flow = Flow([j1, j2])

    submit_flow(flow, worker="test_local_worker")

    # test that a job in batch execution cannot be rerun without force
    job_controller.set_job_state(
        JobState.BATCH_SUBMITTED, job_id=j1.uuid, job_index=j1.index
    )
    with pytest.raises(
        ValueError, match="Rerunning a BATCH_SUBMITTED or BATCH_RUNNING can lead"
    ):
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index)

    assert (
        job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index).state
        == JobState.BATCH_SUBMITTED
    )

    assert job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index, force=True)
    assert (
        job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index).state
        == JobState.READY
    )


@pytest.mark.parametrize("state", ["SUBMITTED", "RUNNING"])
def test_rerun_cancels_queue_process(job_controller, runner, caplog, state) -> None:
    """Rerunning a SUBMITTED or RUNNING Job cancels its process in the queue."""
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add_sleep

    job = add_sleep(1, 30)
    flow = Flow([job])
    submit_flow(flow, worker="test_local_worker")

    runner.run_one_job(max_seconds=20, target_state=JobState.RUNNING)

    qm = runner.get_queue_manager("test_local_worker")
    job_info = job_controller.get_job_info(job_id=job.uuid, job_index=job.index)
    process_id = job_info.remote.process_id
    assert len(qm.get_jobs_list([process_id])) == 1

    if state == "SUBMITTED":
        job_controller.set_job_state(
            JobState.SUBMITTED, job_id=job.uuid, job_index=job.index
        )
    assert (
        job_controller.get_job_info(job_id=job.uuid, job_index=job.index).state.value
        == state
    )

    assert job_controller.rerun_job(job_id=job.uuid, job_index=job.index) == [
        job_info.db_id
    ]

    assert len(qm.get_jobs_list([process_id])) == 0
    assert "Failed cancelling the process" not in caplog.text
    assert (
        job_controller.get_job_info(job_id=job.uuid, job_index=job.index).state
        == JobState.READY
    )


def test_rerun_force_cancels_children_queue_process(
    job_controller, runner, caplog
) -> None:
    """A forced rerun cancels the queue process of a RUNNING descendant."""
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add, add_sleep

    j1 = add(1, 2)
    j2 = add_sleep(j1.output, 30)
    flow = Flow([j1, j2])
    submit_flow(flow, worker="test_local_worker")

    assert runner.run_one_job(max_seconds=10, job_id=[j1.uuid, j1.index])
    runner.run_one_job(
        max_seconds=20, job_id=[j2.uuid, j2.index], target_state=JobState.RUNNING
    )

    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j2_info = job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index)
    assert j1_info.state == JobState.COMPLETED
    assert j2_info.state == JobState.RUNNING

    qm = runner.get_queue_manager("test_local_worker")
    process_id = j2_info.remote.process_id
    assert len(qm.get_jobs_list([process_id])) == 1

    assert set(
        job_controller.rerun_job(job_id=j1.uuid, job_index=j1.index, force=True)
    ) == {j1_info.db_id, j2_info.db_id}

    assert len(qm.get_jobs_list([process_id])) == 0
    assert "Failed cancelling the process" not in caplog.text
    assert (
        job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index).state
        == JobState.READY
    )
    assert (
        job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index).state
        == JobState.WAITING
    )


def test_retry(job_controller, monkeypatch, runner) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add

    j = add(1, 5)
    flow = Flow([j])
    submit_flow(flow, worker="test_local_worker")

    # cannot retry a READY job
    with pytest.raises(ValueError, match="Job in state READY cannot be retried"):
        job_controller.retry_job(job_id=j.uuid, job_index=j.index)

    # patch the upload method of the runner to trigger a remote error
    def upload_error(self, lock) -> NoReturn:
        raise RuntimeError("FAKE ERROR")

    # Run to get the Job the REMOTE_ERROR state
    with monkeypatch.context() as m:
        m.setattr(Runner, "upload", upload_error)
        # patch this to 1 to avoid retrying multiple times
        m.setattr(runner.runner_options, "max_step_attempts", 1)
        with pytest.warns(match="FAKE ERROR"):
            assert runner.run_one_job(max_seconds=10, job_id=(j.uuid, j.index))

    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    assert j_info.state == JobState.REMOTE_ERROR
    assert j_info.remote.retry_time_limit is not None

    assert job_controller.retry_job(job_id=j.uuid, job_index=j.index) == j_info.db_id

    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    assert j_info.state == JobState.CHECKED_OUT
    assert j_info.remote.retry_time_limit is None

    # Run to fail only once
    with monkeypatch.context() as m:
        m.setattr(Runner, "upload", upload_error)
        # patch to make the runner fail only once
        m.setattr(runner.runner_options, "delta_retry", (30, 300, 1200))
        with pytest.warns(match="FAKE ERROR"):
            assert not runner.run_one_job(
                max_seconds=2, job_id=(j.uuid, j.index), raise_at_timeout=False
            )

    j_info = job_controller.get_job_info(job_id=j.uuid, job_index=j.index)
    assert j_info.state == JobState.CHECKED_OUT
    assert j_info.remote.retry_time_limit is not None


def test_pause_play(job_controller) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    j = add(1, 5)
    flow = Flow([j])
    submit_flow(flow, worker="test_local_worker")

    assert job_controller.pause_jobs(job_ids=(j.uuid, 1)) == ["1"]
    assert job_controller.get_job_info(job_id=j.uuid).state == JobState.PAUSED
    assert job_controller.get_flows_info(job_ids=j.uuid)[0].state == FlowState.PAUSED

    assert job_controller.resume_jobs(job_ids=(j.uuid, 1)) == ["1"]
    assert job_controller.get_job_info(job_id=j.uuid).state == JobState.READY
    assert job_controller.get_flows_info(job_ids=j.uuid)[0].state == FlowState.READY


def test_stop(job_controller, one_job, caplog) -> None:
    from jobflow_remote.jobs.state import FlowState, JobState

    j = one_job.jobs[0]
    assert job_controller.stop_jobs(job_ids=(j.uuid, 1)) == ["1"]
    assert job_controller.get_job_info(job_id=j.uuid).state == JobState.USER_STOPPED
    assert job_controller.get_flows_info(job_ids=j.uuid)[0].state == FlowState.STOPPED

    job_controller.set_job_state(JobState.BATCH_SUBMITTED, job_id=j.uuid, job_index=1)
    assert job_controller.stop_jobs(job_ids=(j.uuid, 1)) == ["1"]
    assert job_controller.get_job_info(job_id=j.uuid).state == JobState.USER_STOPPED
    assert "cannot be cancelled from execution on the worker" in caplog.text


@pytest.mark.parametrize(
    "action", ["pause", "stop", "resume", "retry", "rerun", "rerun_force"]
)
def test_actions_accepted_states(job_controller, one_job, action) -> None:
    """
    A Job without children is accepted by each action exactly in the states
    of the action's constant.
    """
    from jobflow_remote.jobs.state import (
        PAUSABLE_STATES,
        RERUNNABLE_STATES,
        RERUNNABLE_STATES_FORCE,
        RESUMABLE_STATES,
        RETRYABLE_STATES,
        STOPPABLE_STATES,
        JobState,
    )

    actions = {
        "pause": (job_controller.pause_job, {}, PAUSABLE_STATES),
        "stop": (job_controller.stop_job, {}, STOPPABLE_STATES),
        "resume": (job_controller.resume_job, {}, RESUMABLE_STATES),
        "retry": (job_controller.retry_job, {}, RETRYABLE_STATES),
        "rerun": (job_controller.rerun_job, {}, RERUNNABLE_STATES),
        "rerun_force": (
            job_controller.rerun_job,
            {"force": True},
            RERUNNABLE_STATES_FORCE,
        ),
    }
    method, kwargs, expected_states = actions[action]
    job = one_job.jobs[0]

    accepted = set()
    for state in JobState:
        # a valid previous_state is required to retry a REMOTE_ERROR Job
        assert job_controller.set_job_doc_properties(
            {"state": state.value, "previous_state": JobState.CHECKED_OUT.value},
            job_id=job.uuid,
        )
        try:
            method(job_id=job.uuid, **kwargs)
        except ValueError:
            continue
        accepted.add(state)

    assert accepted == set(expected_states)


def test_missing_documents_raise_not_found(job_controller) -> None:
    """The actions raise JobNotFoundError or FlowNotFoundError for missing ids."""
    from jobflow_remote.utils.db import FlowNotFoundError, JobNotFoundError

    # lock_job_flow, used by stop, pause, resume and delete
    with pytest.raises(JobNotFoundError):
        job_controller.stop_job(db_id="99999")
    with pytest.raises(JobNotFoundError):
        job_controller.rerun_job(db_id="99999")
    with pytest.raises(JobNotFoundError):
        job_controller.retry_job(db_id="99999")
    with pytest.raises(FlowNotFoundError):
        job_controller.resume_flow(flow_id="no-such-flow")


def test_resume_flow_accepted_states(job_controller, one_job) -> None:
    """A Flow is resumed exactly in the states of RESUMABLE_FLOW_STATES."""
    from jobflow_remote.jobs.state import RESUMABLE_FLOW_STATES, FlowState

    accepted = set()
    for state in FlowState:
        job_controller.flows.update_one(
            {"uuid": one_job.uuid}, {"$set": {"state": state.value}}
        )
        try:
            job_controller.resume_flow(flow_id=one_job.uuid)
        except ValueError:
            continue
        accepted.add(state)

    assert accepted == set(RESUMABLE_FLOW_STATES)


def test_unlock_jobs(job_controller, one_job) -> None:
    j = one_job.jobs[0]
    # catch the warning coming from MongoLock
    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        job_controller.lock_job(filter={"uuid": j.uuid}),
    ):
        assert job_controller.unlock_jobs(job_ids=(j.uuid, 1)) == 1


def test_unlock_flows(job_controller, one_job) -> None:
    # catch the warning coming from MongoLock
    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        job_controller.lock_flow(filter={"uuid": one_job.uuid}),
    ):
        assert job_controller.unlock_flows(flow_ids=one_job.uuid) == 1


def test_set_job_run_properties(job_controller, one_job) -> None:
    from qtoolkit import QResources
    from qtoolkit.core.data_objects import ProcessPlacement

    from jobflow_remote.config.base import ExecutionConfig

    # test setting worker
    with pytest.raises(
        ValueError, match="worker missing_worker is not present in the project"
    ):
        job_controller.set_job_run_properties(worker="missing_worker")

    assert job_controller.set_job_run_properties(worker="test_local_worker_2")
    assert (
        job_controller.get_job_info(job_id=one_job[0].uuid).worker
        == "test_local_worker_2"
    )

    # test setting exec config
    with pytest.raises(
        ValueError,
        match="exec_config missing_exec_config is not present in the project",
    ):
        job_controller.set_job_run_properties(exec_config="missing_exec_config")

    assert job_controller.set_job_run_properties(exec_config="test")
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).exec_config == "test"

    ec1 = ExecutionConfig(modules=["some_module"])
    ec2 = ExecutionConfig(pre_run="command")
    ec3 = ExecutionConfig(modules=["some_module"], pre_run="command")
    assert job_controller.set_job_run_properties(exec_config=ec1)
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).exec_config == ec1

    assert job_controller.set_job_run_properties(exec_config=ec2, update=False)
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).exec_config == ec2

    assert job_controller.set_job_run_properties(
        exec_config={"modules": ["some_module"]}, update=True
    )
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).exec_config == ec3

    # test setting resources
    qr = QResources(
        queue_name="test", process_placement=ProcessPlacement.NO_CONSTRAINTS
    )

    assert job_controller.set_job_run_properties(resources={"ntasks": 10})
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).resources == {
        "ntasks": 10
    }
    assert job_controller.set_job_run_properties(resources={"nodes": 10})
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).resources == {
        "ntasks": 10,
        "nodes": 10,
    }
    assert job_controller.set_job_run_properties(resources={"ntasks": 20}, update=False)
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).resources == {
        "ntasks": 20
    }

    assert job_controller.set_job_run_properties(resources=qr)
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).resources == qr

    assert job_controller.set_job_run_properties(priority=10)
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).priority == 10
    assert job_controller.set_job_run_properties()
    assert job_controller.get_job_doc(job_id=one_job[0].uuid).priority == 10


def test_set_job_doc_properties(job_controller, one_job) -> None:
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.utils.db import JobLockedError, JobNotFoundError

    # error missing input
    with pytest.raises(
        ValueError, match="One and only one among job_id and db_id should be defined"
    ):
        job_controller.set_job_doc_properties(
            values={"job.metadata.x": "y"}, acceptable_states=[JobState.COMPLETED]
        )

    # error wrong state
    with pytest.raises(
        ValueError, match="Job in state READY. The action cannot be performed"
    ):
        job_controller.set_job_doc_properties(
            values={"job.metadata.x": "y"},
            job_id=one_job[0].uuid,
            acceptable_states=[JobState.COMPLETED],
        )

    # error missing job
    with pytest.raises(JobNotFoundError, match="No Job matching criteria"):
        job_controller.set_job_doc_properties(
            values={"job.metadata.x": "y"},
            job_id="wrong_uuid",
        )
    # error locked job
    with (
        pytest.raises(JobLockedError, match="The Job matching criteria.*is locked"),
        job_controller.lock_job(
            filter={"uuid": one_job[0].uuid, "index": one_job[0].index}
        ),
    ):
        job_controller.set_job_doc_properties(
            values={"job.metadata.x": "y"},
            job_id=one_job[0].uuid,
            job_index=one_job[0].index,
        )

    job_controller.set_job_doc_properties(
        values={"job.metadata.x": "y"},
        job_id=one_job[0].uuid,
        acceptable_states=[JobState.READY],
    )

    assert job_controller.get_job_doc(job_id=one_job[0].uuid).job.metadata == {"x": "y"}


def test_reset(job_controller, two_flows_four_jobs):
    from datetime import datetime

    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add

    assert job_controller.count_jobs() == 4

    assert not job_controller.reset(max_limit=1)
    assert job_controller.count_jobs() == 4
    assert job_controller.reset(max_limit=10, reset_output=True)

    assert job_controller.count_jobs() == 0

    for _ in range(4):
        f = Flow(add(1, 2))
        submit_flow(f, worker="test_local_worker")

    assert job_controller.count_jobs() == 4
    assert not job_controller.reset(max_limit=1, validation="1327-01-01")
    assert job_controller.count_jobs() == 4
    assert job_controller.reset(
        max_limit=1, validation=datetime.now().strftime("%Y-%m-%d")
    )
    assert job_controller.count_jobs() == 0


def test_delete_job_unreachable_worker(
    job_controller, runner, caplog, monkeypatch
) -> None:
    """
    A worker that cannot be reached does not stop the deletion of the Job:
    the Job and its output are deleted, the files are not, and the folder is
    logged.
    """
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.remote.host.local import LocalHost
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    j2 = add(j1.output, 4)
    flow = Flow([j1, j2])
    submit_flow(flow, worker="test_local_worker")
    runner.run_all_jobs(max_seconds=20)

    j2_info = job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index)
    assert os.path.isdir(j2_info.run_dir)

    def unreachable_connect(self) -> None:
        raise TimeoutError("timed out")

    monkeypatch.setattr(LocalHost, "connect", unreachable_connect)

    assert (
        job_controller.delete_job(job_id=j2.uuid, delete_output=True, delete_files=True)
        == j2_info.db_id
    )

    assert job_controller.count_jobs(job_ids=[(j2.uuid, j2.index)]) == 0
    assert job_controller.count_jobs(flow_ids=[flow.uuid]) == 1
    with pytest.raises(ValueError, match=".*has no outputs.*"):
        job_controller.jobstore.get_output(j2.uuid)
    assert os.path.isdir(j2_info.run_dir)
    assert (
        f"The files of Job {j2_info.db_id} in {j2_info.run_dir} were not deleted"
    ) in caplog.text


# the Jobs are independent, so that they can run at the same time
@pytest.mark.filterwarnings("ignore:Some jobs are not connected")
def test_delete_job_cancels_queue_process(job_controller, runner, caplog) -> None:
    """Deleting a RUNNING Job cancels its process in the queue, unless disabled."""
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add, add_sleep

    j_cancel = add_sleep(1, 30)
    j_keep = add_sleep(2, 30)
    j_other = add(1, 2)
    flow = Flow([j_cancel, j_keep, j_other])
    submit_flow(flow, worker="test_local_worker")

    for j in (j_cancel, j_keep):
        runner.run_one_job(
            max_seconds=20, job_id=[j.uuid, j.index], target_state=JobState.RUNNING
        )
    qm = runner.get_queue_manager("test_local_worker")
    cancel_info = job_controller.get_job_info(job_id=j_cancel.uuid)
    keep_info = job_controller.get_job_info(job_id=j_keep.uuid)
    assert cancel_info.state == JobState.RUNNING
    assert keep_info.state == JobState.RUNNING
    assert len(qm.get_jobs_list([cancel_info.remote.process_id])) == 1
    assert len(qm.get_jobs_list([keep_info.remote.process_id])) == 1

    assert job_controller.delete_job(job_id=j_cancel.uuid) == cancel_info.db_id
    assert len(qm.get_jobs_list([cancel_info.remote.process_id])) == 0
    assert "Failed cancelling the process" not in caplog.text

    assert (
        job_controller.delete_job(job_id=j_keep.uuid, cancel_processes=False)
        == keep_info.db_id
    )
    assert len(qm.get_jobs_list([keep_info.remote.process_id])) == 1
    # cancel it to avoid leaving the process running after the test
    qm.cancel(keep_info.remote.process_id)

    assert job_controller.count_jobs(flow_ids=[flow.uuid]) == 1


def test_delete_job(job_controller, two_flows_four_jobs, runner):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState
    from jobflow_remote.testing import add

    runner.run_one_job(job_id=[two_flows_four_jobs[0][0].uuid, 1])
    flow_doc = job_controller.get_flow_info_by_flow_uuid(two_flows_four_jobs[0].uuid)
    assert flow_doc["state"] == FlowState.RUNNING.value

    assert len(job_controller.delete_job(job_id=two_flows_four_jobs[0][1].uuid)) == 1
    assert job_controller.count_jobs() == 3
    flow_doc = job_controller.get_flow_info_by_flow_uuid(two_flows_four_jobs[0].uuid)
    assert flow_doc["state"] == FlowState.COMPLETED.value
    assert len(flow_doc["jobs"]) == 1
    assert len(flow_doc["ids"]) == 1
    assert len(flow_doc["parents"]) == 1
    assert len(flow_doc["parents"][two_flows_four_jobs[0][0].uuid]["1"]) == 0

    assert job_controller.jobstore.get_output(two_flows_four_jobs[0][0].uuid) == 6

    # try deleting the last job of the Flow. should not succeed
    with pytest.raises(
        RuntimeError, match="It is not possible to delete the only Job of the Flow.*"
    ):
        job_controller.delete_job(job_id=two_flows_four_jobs[0][0].uuid)

    # delete a completed job
    runner.run_one_job(job_id=[two_flows_four_jobs[1][0].uuid, 1])

    job3_info = job_controller.get_job_info(two_flows_four_jobs[1][0].uuid)

    assert os.path.isdir(job3_info.run_dir)

    assert (
        len(
            job_controller.delete_job(
                job_id=two_flows_four_jobs[1][0].uuid,
                delete_output=True,
                delete_files=True,
            )
        )
        == 1
    )
    with pytest.raises(ValueError, match=".*has no outputs.*"):
        job_controller.jobstore.get_output(two_flows_four_jobs[1][0].uuid)

    assert not os.path.isdir(job3_info.run_dir)

    # delete a completed job with delete_jobs
    add1 = add(1, 2)
    add2 = add(add1.output, 2)
    new_flow = Flow([add1, add2])
    submit_flow(new_flow)
    runner.run_all_jobs()
    add2_info = job_controller.get_job_info(add2.uuid)
    assert job_controller.jobstore.get_output(add2.uuid)
    assert os.path.isdir(add2_info.run_dir)
    assert (
        len(
            job_controller.delete_jobs(
                job_ids=[(add2.uuid, 1)], delete_output=True, delete_files=True
            )
        )
        == 1
    )
    with pytest.raises(ValueError, match=".*has no outputs.*"):
        job_controller.jobstore.get_output(add2.uuid)

    assert not os.path.isdir(add2_info.run_dir)

    # add a big flow and try deleting it with small limit
    many_jobs = [add(1, 2)]
    for i in range(12):
        many_jobs.append(add(many_jobs[-1].output, i))
    large_flow = Flow(many_jobs)
    submit_flow(large_flow)
    job_ids = [(j.uuid, 1) for j in many_jobs[1:]]
    with pytest.raises(ValueError, match="Cannot perform deleting on 12 Jobs"):
        job_controller.delete_jobs(job_ids)

    # increase the limit and delete
    assert len(job_controller.delete_jobs(job_ids, max_limit=20)) == 12
    flow_doc = job_controller.get_flow_info_by_flow_uuid(large_flow.uuid)
    assert len(flow_doc["ids"]) == 1


def test_delete_job_with_replace(job_controller, runner):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState
    from jobflow_remote.testing import add, self_replace

    # create a job with a replace to check that the job deletion is handled correctly,
    # although the final state may not be consistent
    j1 = self_replace(1)
    j2 = add(j1.output, 2)
    flow = Flow([j1, j2])

    submit_flow(flow, worker="test_local_worker")
    runner.run_all_jobs()

    flow_doc = job_controller.get_flow_info_by_flow_uuid(flow.uuid)
    assert flow_doc["state"] == FlowState.COMPLETED.value
    assert len(flow_doc["jobs"]) == 2
    assert len(flow_doc["ids"]) == 3
    assert flow_doc["parents"][j2.uuid]["1"] == [j1.uuid]
    assert job_controller.count_jobs() == 3

    job1_info = job_controller.get_job_info(j1.uuid)
    assert len(job_controller.delete_job(job_id=j1.uuid, job_index=2)) == 1
    assert os.path.isdir(job1_info.run_dir)

    assert job_controller.count_jobs() == 2
    flow_doc = job_controller.get_flow_info_by_flow_uuid(flow.uuid)
    assert flow_doc["state"] == FlowState.COMPLETED.value
    assert len(flow_doc["jobs"]) == 2
    assert len(flow_doc["ids"]) == 2
    assert len(flow_doc["parents"]) == 2
    assert flow_doc["parents"][j2.uuid]["1"] == [j1.uuid]

    # now delete also the job with index 1 and check again
    assert len(job_controller.delete_job(job_id=j1.uuid, job_index=1)) == 1
    assert job_controller.count_jobs() == 1
    flow_doc = job_controller.get_flow_info_by_flow_uuid(flow.uuid)
    assert flow_doc["state"] == FlowState.COMPLETED.value
    assert len(flow_doc["jobs"]) == 1
    assert len(flow_doc["ids"]) == 1
    assert len(flow_doc["parents"]) == 1
    assert flow_doc["parents"][j2.uuid]["1"] == []


def test_add_flow(job_controller):
    from jobflow import Flow

    from jobflow_remote.testing import add

    j = add(1, 2)
    flow = Flow([j])

    job_controller.add_flow(flow, worker="test_local_worker")
    assert job_controller.count_jobs() == 1

    # the same flow cannot be inserted twice
    with pytest.raises(
        ValueError, match=".*A duplicate key error happened while inserting the flow.*"
    ):
        job_controller.add_flow(flow, worker="test_local_worker")
    assert job_controller.count_jobs() == 1


def test_get_indexes(job_controller, one_job):
    from jobflow_remote.jobs.data import DbCollection

    # jobs is the default collection
    job_indexes = job_controller.get_indexes()
    assert job_indexes == job_controller.jobs.index_information()
    assert job_indexes["db_id_1"]["unique"]
    assert job_indexes["uuid_1_index_1"]["key"] == [("uuid", 1), ("index", 1)]
    assert "job.hosts_1" in job_indexes

    flow_indexes = job_controller.get_indexes(DbCollection.FLOWS)
    assert flow_indexes == job_controller.flows.index_information()
    assert "created_on_1" in flow_indexes

    # a newly created index is listed
    job_controller.create_indexes(
        [[("xxx", pymongo.DESCENDING), ("yyy", pymongo.ASCENDING)]],
        collection=DbCollection.FLOWS,
        background=False,
    )
    flow_indexes = job_controller.get_indexes(DbCollection.FLOWS)
    assert flow_indexes["xxx_-1_yyy_1"]["key"] == [("xxx", -1), ("yyy", 1)]


def test_indexes(job_controller, one_job):
    job_indexes = job_controller.jobs.index_information()
    assert len(job_indexes) == 11
    assert "job.hosts_1" in job_indexes
    # uuid alone is served by the prefix of the compound (uuid, index) index
    assert "uuid_1" not in job_indexes
    assert "uuid_1_index_1" in job_indexes

    flow_indexes = job_controller.flows.index_information()
    assert len(flow_indexes) == 8
    assert "created_on_1" in flow_indexes

    job_controller.create_indexes(
        [[("xxx", pymongo.DESCENDING), ("yyy", pymongo.ASCENDING)]],
        unique=True,
        background=False,
    )
    assert len(list(job_controller.jobs.list_indexes())) == 12

    job_controller.build_indexes(background=False, drop=True)
    assert len(list(job_controller.jobs.list_indexes())) == 11


@pytest.mark.parametrize(
    "python",
    [
        True,
        pytest.param(
            False,
            marks=pytest.mark.skipif(
                not which("mongodump"), reason="mongodump missing"
            ),
        ),
    ],
)
@pytest.mark.parametrize(
    "compress",
    [True, False],
)
def test_backup(job_controller_drop, python, compress):
    import tempfile
    from pathlib import Path

    from jobflow import Flow
    from maggma.stores.mongolike import MemoryStore

    from jobflow_remote.testing import add

    # modify the jobcontroller so that the collections are not the standard ones
    jobs_name = "test_jobs"
    flows_name = "test_flows"
    aux_name = "test_auxiliary"
    batches_name = "test_batches"
    job_controller_drop.jobs_collection = jobs_name
    job_controller_drop.jobs = job_controller_drop.db[jobs_name]
    job_controller_drop.queue_store.collection_name = jobs_name
    job_controller_drop.queue_store._coll = job_controller_drop.jobs
    job_controller_drop.flows_collection = flows_name
    job_controller_drop.flows = job_controller_drop.db[flows_name]
    job_controller_drop.auxiliary_collection = aux_name
    job_controller_drop.auxiliary = job_controller_drop.db[aux_name]
    job_controller_drop.batches_collection = batches_name
    job_controller_drop.batches = job_controller_drop.db[batches_name]

    job_controller_drop.reset()

    j1 = add(1, 2)
    j2 = add(j1.output, 2)
    flow = Flow([j1, j2])

    # do not use submit_flow, otherwise the original collections will be used.
    job_controller_drop.add_flow(flow, worker="test_local_worker")
    # Manually add a fake batch process
    job_controller_drop.add_batch_process(
        process_id="fake_bp_id",
        batch_uid="fake_bp_uid",
        worker="test_local_batch_worker",
    )

    assert job_controller_drop.count_jobs() == 2
    assert job_controller_drop.count_batches(worker="test_local_worker") == 0
    assert job_controller_drop.count_batches(worker="test_local_batch_worker") == 1
    assert job_controller_drop.count_batches() == 1

    db_name = job_controller_drop.queue_store.database
    with tempfile.TemporaryDirectory() as dir_name:
        dir_path = Path(dir_name)

        job_controller_drop.backup_dump(
            dir_path=dir_name, compress=compress, python=python
        )

        files = [str(p.name) for p in (dir_path / db_name).glob("*")]
        ext = ".gz" if compress else ""
        if "prelude.json" + ext in files:
            files = list(files)
            files.remove("prelude.json" + ext)

        ext = ".gz" if compress else ""
        assert "flows.bson" + ext in files
        assert "jf_auxiliary.bson" + ext in files
        assert "jobs.bson" + ext in files
        assert "batches.bson" + ext in files
        if python:
            assert len(files) == 4
        else:
            assert "flows.metadata.json" + ext in files
            assert "jf_auxiliary.metadata.json" + ext in files
            assert "jobs.metadata.json" + ext in files

            assert len(files) == 8

        with pytest.raises(
            RuntimeError,
            match=f"The collection named {jobs_name} for jobs contains 2 documents.*",
        ):
            job_controller_drop.backup_restore(
                dir_path=dir_path / db_name, compress=compress, python=python
            )

        job_controller_drop.jobs.delete_many({})
        with pytest.raises(
            RuntimeError,
            match=f"The collection named {flows_name} for flows contains 1 documents.*",
        ):
            job_controller_drop.backup_restore(
                dir_path=dir_path / db_name, compress=compress, python=python
            )

        job_controller_drop.flows.delete_many({})

        with pytest.raises(
            RuntimeError,
            match=f"The collection named {batches_name} for batches contains 1 documents.*",
        ):
            job_controller_drop.backup_restore(
                dir_path=dir_path / db_name, compress=compress, python=python
            )

        job_controller_drop.batches.delete_many({})
        with pytest.raises(
            RuntimeError,
            match="next_id value is different from one in the auxiliary collection.*",
        ):
            job_controller_drop.backup_restore(
                dir_path=dir_path / db_name, compress=compress, python=python
            )

        job_controller_drop.auxiliary.delete_many({})

        job_controller_drop.backup_restore(dir_path=dir_path / db_name, python=python)

        assert job_controller_drop.count_jobs() == 2
        assert job_controller_drop.count_flows() == 1
        assert job_controller_drop.auxiliary.find({})[0]["next_id"] == 3

        # Other stores not supported for mongodump
        if not python:
            job_controller_drop.queue_store = MemoryStore()
            with pytest.raises(
                ValueError, match="Unsupported store type MemoryStore.*"
            ):
                job_controller_drop.backup_dump(
                    dir_path=dir_name, compress=compress, python=python
                )


def test_get_assigned_workers(job_controller) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.testing import add

    assert job_controller.get_assigned_workers() == []

    submit_flow(Flow([add(1, 2), add(3, 4)]), worker="test_local_worker")
    submit_flow(Flow([add(1, 2)]), worker="test_local_batch_worker")
    submit_flow(Flow([add(1, 2)]), worker="test_local_worker")

    # each worker once, sorted
    assert job_controller.get_assigned_workers() == [
        "test_local_batch_worker",
        "test_local_worker",
    ]

    # a worker no longer in the configuration is still listed, while
    # a worker without Jobs is not
    job_controller.jobs.update_one(
        {"worker": "test_local_batch_worker"},
        {"$set": {"worker": "a_removed_worker"}},
    )
    assert job_controller.get_assigned_workers() == [
        "a_removed_worker",
        "test_local_worker",
    ]


def test_count_states(job_controller):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.runner import Runner
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    j2 = add(j1.output, 2)
    flow = Flow([j1, j2])

    # test both job and flow state count to avoid rerunning the job.
    submit_flow(flow, worker="test_local_worker")

    jobs_states = job_controller.count_jobs_states(list(JobState))
    assert jobs_states[JobState.COMPLETED] == 0
    assert jobs_states[JobState.READY] == 1
    assert jobs_states[JobState.RUNNING] == 0
    assert jobs_states[JobState.WAITING] == 1

    flows_states = job_controller.count_flows_states(list(FlowState))
    assert flows_states[FlowState.COMPLETED] == 0
    assert flows_states[FlowState.READY] == 1
    assert flows_states[FlowState.RUNNING] == 0

    runner = Runner()
    runner.run_one_job()

    jobs_states = job_controller.count_jobs_states(list(JobState))
    assert jobs_states[JobState.COMPLETED] == 1
    assert jobs_states[JobState.READY] == 1
    assert jobs_states[JobState.RUNNING] == 0

    flows_states = job_controller.count_flows_states(list(FlowState))
    assert flows_states[FlowState.COMPLETED] == 0
    assert flows_states[FlowState.READY] == 0
    assert flows_states[FlowState.RUNNING] == 1


def test_get_trends(job_controller, one_job):
    from datetime import datetime, timezone

    import dateutil

    from jobflow_remote.jobs.state import FlowState, JobState

    tzname = datetime.now(dateutil.tz.tzlocal()).tzname()

    now = datetime.now()
    utcnow = datetime.now(timezone.utc)
    job_trends = job_controller.get_trends(
        list(JobState), interval="days", interval_timezone=tzname
    )
    assert len(job_trends) == 7
    assert now.strftime("%Y-%m-%d") in job_trends
    assert job_trends[now.strftime("%Y-%m-%d")][JobState.READY] == 1
    assert job_trends[now.strftime("%Y-%m-%d")][JobState.COMPLETED] == 0

    job_trends = job_controller.get_trends(
        list(JobState), interval="hours", num_intervals=5, interval_timezone=tzname
    )
    assert len(job_trends) == 5
    assert now.strftime("%Y-%m-%d %H") in job_trends
    assert job_trends[now.strftime("%Y-%m-%d %H")][JobState.READY] == 1
    assert job_trends[now.strftime("%Y-%m-%d %H")][JobState.COMPLETED] == 0

    job_trends = job_controller.get_trends(
        list(JobState), interval="weeks", interval_timezone="UTC"
    )
    assert len(job_trends) == 4
    iso_year, iso_week, _ = utcnow.isocalendar()
    iso_id = f"{iso_year}-{iso_week:02d}"
    assert iso_id in job_trends
    assert job_trends[iso_id][JobState.READY] == 1
    assert job_trends[iso_id][JobState.COMPLETED] == 0

    job_trends = job_controller.get_trends(
        list(JobState), interval="years", num_intervals=2, interval_timezone=tzname
    )
    assert len(job_trends) == 2
    assert now.strftime("%Y") in job_trends
    assert job_trends[now.strftime("%Y")][JobState.READY] == 1
    assert job_trends[now.strftime("%Y")][JobState.COMPLETED] == 0

    flow_trends = job_controller.get_trends(
        list(FlowState), interval="days", interval_timezone=tzname
    )
    assert len(flow_trends) == 7
    assert now.strftime("%Y-%m-%d") in flow_trends
    assert flow_trends[now.strftime("%Y-%m-%d")][FlowState.READY] == 1
    assert flow_trends[now.strftime("%Y-%m-%d")][FlowState.COMPLETED] == 0


def test_running_runner(job_controller, daemon_manager, wait_daemon_started):
    from jobflow_remote.utils.db import LockedDocumentError, MissingDocumentError

    assert job_controller.get_running_runner() is None
    daemon_manager.start(single=True)
    wait_daemon_started(daemon_manager)
    assert job_controller.get_running_runner() is not None

    with (
        pytest.warns(UserWarning, match="Could not release lock for document"),
        job_controller.lock_auxiliary(filter={"running_runner": {"$exists": True}}),
    ):
        with pytest.raises(LockedDocumentError):
            job_controller.clean_running_runner()

        job_controller.clean_running_runner(break_lock=True)
        assert job_controller.get_running_runner() is None

    job_controller.auxiliary.delete_one({"running_runner": {"$exists": True}})
    with pytest.raises(
        MissingDocumentError, match="Runner document missing from auxiliary collection"
    ):
        job_controller.clean_running_runner()
    assert job_controller.get_running_runner() == "NO_DOCUMENT"


def test_runner_pings(job_controller, monkeypatch):
    import jobflow_remote.jobs.jobcontroller

    with monkeypatch.context() as m:
        m.setattr(jobflow_remote.jobs.jobcontroller, "_MAX_ARCHIVED_RUNNER_PINGS", 3)
        job_controller.ping_running_runner(data={"test": "1"})
        job_controller.ping_running_runner(data={"test": "2"})
        assert len(job_controller.get_runner_pings()) == 2
        for i in range(3):
            job_controller.ping_running_runner(data={"test": str(i + 3)})
        pings = job_controller.get_runner_pings()
        assert len(pings) == 3
        assert [d["test"] for d in pings] == ["3", "4", "5"]
        job_controller.clean_runner_pings()
        assert len(job_controller.get_runner_pings()) == 0


def test_stop_jobflow_resume(job_controller, runner) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add, replace_and_stop_jobflow

    # add other jobs to verify the difference between stop_joblow and stop_children
    j_add = add(1, 1)
    j_stop = replace_and_stop_jobflow()
    j_final = add(j_add.output, j_stop.output)
    flow = Flow([j_add, j_stop, j_final])
    submit_flow(flow=flow, worker="test_local_worker")

    runner.run_one_job(job_id=[j_stop.uuid, 1])

    assert job_controller.count_jobs() == 6
    assert (
        job_controller.get_jobs_info(job_ids=[j_stop.uuid, 1])[0].state
        == JobState.COMPLETED
    )
    assert job_controller.count_jobs(states=JobState.STOPPED) == 5
    assert job_controller.get_flows_info()[0].state == FlowState.STOPPED

    job_controller.resume_job(job_id=j_add.uuid)
    assert (
        job_controller.get_jobs_info(job_ids=[j_add.uuid, 1])[0].state == JobState.READY
    )
    job_controller.resume_job(job_id=j_final.uuid)
    assert (
        job_controller.get_jobs_info(job_ids=[j_final.uuid, 1])[0].state
        == JobState.WAITING
    )
    assert job_controller.get_flows_info()[0].state == FlowState.STOPPED


def test_stop_children_resume(job_controller, runner) -> None:
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add, replace_and_stop_children

    j_add = add(1, 1)
    j_stop_children = replace_and_stop_children()
    j_final = add(j_add.output, j_stop_children.output)
    flow = Flow([j_add, j_stop_children, j_final])
    submit_flow(flow=flow, worker="test_local_worker")

    runner.run_one_job(job_id=[j_stop_children.uuid, 1])

    assert job_controller.count_jobs() == 6
    assert (
        job_controller.get_jobs_info(job_ids=[j_stop_children.uuid, 1])[0].state
        == JobState.COMPLETED
    )
    assert (
        job_controller.get_jobs_info(job_ids=[j_add.uuid, 1])[0].state == JobState.READY
    )
    assert job_controller.count_jobs(states=JobState.STOPPED) == 4
    assert job_controller.get_flows_info()[0].state == FlowState.STOPPED

    job_controller.resume_jobs(states=JobState.STOPPED)
    assert job_controller.count_jobs(states=JobState.STOPPED) == 0
    assert job_controller.count_jobs(states=JobState.READY) == 2
    assert job_controller.count_jobs(states=JobState.WAITING) == 3
    assert job_controller.get_flows_info()[0].state == FlowState.RUNNING


def test_resume_flow(job_controller, runner):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add, replace_and_stop_children

    j_add = add(1, 1)
    j_stop_children = replace_and_stop_children()
    j_final = add(j_add.output, j_stop_children.output)
    flow = Flow([j_add, j_stop_children, j_final])
    submit_flow(flow=flow, worker="test_local_worker")

    runner.run_one_job(job_id=[j_stop_children.uuid, 1])

    assert job_controller.count_jobs() == 6
    assert (
        job_controller.get_jobs_info(job_ids=[j_stop_children.uuid, 1])[0].state
        == JobState.COMPLETED
    )
    assert (
        job_controller.get_jobs_info(job_ids=[j_add.uuid, 1])[0].state == JobState.READY
    )
    assert job_controller.count_jobs(states=JobState.STOPPED) == 4
    assert job_controller.get_flows_info()[0].state == FlowState.STOPPED

    job_controller.pause_job(job_id=j_add.uuid, job_index=1)
    assert job_controller.get_flows_info()[0].state == FlowState.STOPPED
    assert (
        job_controller.get_jobs_info(job_ids=[j_add.uuid, 1])[0].state
        == JobState.PAUSED
    )
    assert job_controller.resume_flow(flow_id=flow.uuid) == 5

    assert job_controller.get_flows_info()[0].state == FlowState.RUNNING
    assert job_controller.count_jobs(states=[JobState.STOPPED, JobState.PAUSED]) == 0

    # the message names the state
    with pytest.raises(ValueError, match=r"^Cannot resume a Flow in state RUNNING$"):
        job_controller.resume_flow(flow_id=flow.uuid)


def test_flow_store(job_controller, two_flows_four_jobs, runner):
    from jobflow_remote.jobs.state import FlowState
    from jobflow_remote.utils.db import FlowNotFoundError

    job_controller.set_flow_store("other_jobstore", flow_id=two_flows_four_jobs[0].uuid)

    assert (
        job_controller.get_flow_store(flow_id=two_flows_four_jobs[0].uuid)
        == "other_jobstore"
    )
    assert job_controller.get_flow_store(flow_id=two_flows_four_jobs[1].uuid) is None

    with pytest.raises(
        ValueError,
        match=".*Store wrong_jobstore is not defined as an optional jobstore.*",
    ):
        job_controller.set_flow_store(
            "wrong_jobstore", flow_id=two_flows_four_jobs[0].uuid
        )

    with pytest.raises(FlowNotFoundError, match="No Flow document matching criteria"):
        job_controller.set_flow_store("other_jobstore", flow_id="wrong_id")

    job_controller.set_flow_store("other_jobstore", flow_id=two_flows_four_jobs[1].uuid)
    assert (
        job_controller.get_flow_store(flow_id=two_flows_four_jobs[1].uuid)
        == "other_jobstore"
    )
    job_controller.set_flow_store(None, flow_id=two_flows_four_jobs[1].uuid)
    assert job_controller.get_flow_store(flow_id=two_flows_four_jobs[1].uuid) is None

    with pytest.raises(FlowNotFoundError, match="No Flow matching id"):
        job_controller.get_flow_store(flow_id="wrong_id")

    runner.run_one_job(job_id=(two_flows_four_jobs[0][0].uuid, 1))

    assert (
        job_controller.get_flows_info(flow_ids=[two_flows_four_jobs[0].uuid])[0].state
        == FlowState.RUNNING
    )
    with pytest.raises(
        RuntimeError, match=".*The JobStore can be set only for a READY Flow.*"
    ):
        job_controller.set_flow_store(
            "other_jobstore", flow_id=two_flows_four_jobs[0].uuid
        )

    # test get_job_output from the additional store
    assert job_controller.get_job_output(job_id=two_flows_four_jobs[0][0].uuid) == 6


def test_get_job_output(job_controller, runner, one_job):
    # here it is tested with the standard jobstore. The optional jobstore is
    # tested in test_flow_store

    with pytest.raises(
        ValueError, match=rf".*UUID: {one_job[0].uuid} \(index: 1\) has no outputs.*"
    ):
        job_controller.get_job_output(db_id="1")

    runner.run_all_jobs(max_seconds=20)

    assert job_controller.get_job_output(db_id="1") == 6


def test_delete_flow(job_controller, runner, caplog):
    from pathlib import Path

    from jobflow import Flow
    from qtoolkit.core.data_objects import CancelStatus

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add, add_sleep

    j1 = add(5, 6)
    f1 = Flow([j1])
    submit_flow(f1, worker="test_local_worker")

    assert job_controller.count_flows() == 1
    assert job_controller.count_jobs() == 1

    assert job_controller.delete_flow(f1.uuid)
    assert job_controller.count_flows() == 0
    assert job_controller.count_jobs() == 0

    # Test delete with output and files
    completed_job = add(10, 20)
    completed_flow = Flow([completed_job])
    submit_flow(completed_flow, worker="test_local_worker")

    # Run to completion to create output and files
    runner.run_all_jobs(max_seconds=10)

    completed_job_info = job_controller.get_job_info(
        job_id=completed_job.uuid, job_index=completed_job.index
    )
    assert completed_job_info.state == JobState.COMPLETED

    # Verify output exists
    output = job_controller.get_job_output(job_id=completed_job.uuid)
    assert output == 30

    # Verify run directory exists
    run_dir = Path(completed_job_info.run_dir)
    assert run_dir.exists()

    # Delete with delete_output and delete_files
    result = job_controller.delete_flow(
        completed_flow.uuid, delete_output=True, delete_files=True
    )
    assert result is True

    # Verify output was deleted
    with pytest.raises(ValueError, match=".*has no outputs.*"):
        job_controller.jobstore.get_output(completed_job.uuid)

    # Test process cancellation by simulating jobs in SUBMITTED and RUNNING states
    j2 = add_sleep(1, 30)
    f2 = Flow([j2])
    submit_flow(f2, worker="test_local_worker")

    runner.run_one_job(max_seconds=20, target_state=JobState.RUNNING)
    j2_info = job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index)
    assert j2_info.state == JobState.RUNNING

    qm = runner.get_queue_manager("test_local_worker")
    assert qm.get_jobs_list([j2_info.remote.process_id])

    # delete and cancel the remote processes
    assert job_controller.delete_flow(f2.uuid, cancel_processes=True)

    assert job_controller.count_flows() == 0

    assert not qm.get_jobs_list([j2_info.remote.process_id])

    # Test cancel_processes=False
    j3 = add(1, 30)
    f3 = Flow([j3])
    submit_flow(f3, worker="test_local_worker")

    runner.run_one_job(max_seconds=20, target_state=JobState.RUNNING)
    j3_info = job_controller.get_job_info(job_id=j3.uuid, job_index=j3.index)
    assert j3_info.state == JobState.RUNNING

    assert qm.get_jobs_list([j3_info.remote.process_id])

    assert job_controller.delete_flow(f3.uuid, cancel_processes=False)

    assert qm.get_jobs_list([j3_info.remote.process_id])

    # attempt cancelling the job to speed up the completion of the test. it is not strictly necessary that it succeeds
    qm.cancel(j3_info.remote.process_id)

    assert job_controller.count_flows() == 0

    # Test cancel_processes=True with already finished Job
    j4 = add(1, 30)
    f4 = Flow([j4])
    submit_flow(f4, worker="test_local_worker")

    runner.run_one_job(max_seconds=20, target_state=JobState.RUNNING)
    j4_info = job_controller.get_job_info(job_id=j4.uuid, job_index=j4.index)
    assert j4_info.state == JobState.RUNNING

    assert qm.get_jobs_list([j4_info.remote.process_id])
    assert qm.cancel(j4_info.remote.process_id).status == CancelStatus.SUCCESSFUL
    assert not qm.get_jobs_list([j4_info.remote.process_id])

    assert job_controller.delete_flow(f4.uuid, cancel_processes=True)

    warning_records = [
        r
        for r in caplog.records
        if "Failed cancelling the process for Job" in r.message
    ]
    assert len(warning_records) == 1

    assert job_controller.count_flows() == 0

    # Test deleting non-existent flow
    assert not job_controller.delete_flow("non-existent-uuid")


def test_delete_flow_unreachable_worker(
    job_controller, runner, caplog, monkeypatch
) -> None:
    """
    A worker that cannot be reached does not stop the deletion of the Flow:
    the Flow, its Jobs and their outputs are deleted, the files are not, and
    their folders are logged.
    """
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.remote.host.local import LocalHost
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    j2 = add(j1.output, 4)
    flow = Flow([j1, j2])
    submit_flow(flow, worker="test_local_worker")
    runner.run_all_jobs(max_seconds=20)

    jobs_info = job_controller.get_jobs_info(flow_ids=[flow.uuid])
    assert {ji.state for ji in jobs_info} == {JobState.COMPLETED}
    for ji in jobs_info:
        assert os.path.isdir(ji.run_dir)

    connect_calls = []

    def unreachable_connect(self) -> None:
        connect_calls.append(self)
        raise TimeoutError("timed out")

    monkeypatch.setattr(LocalHost, "connect", unreachable_connect)

    assert job_controller.delete_flow(flow.uuid, delete_output=True, delete_files=True)

    # the documents and the outputs are deleted
    assert job_controller.count_flows(flow_ids=[flow.uuid]) == 0
    assert job_controller.count_jobs(flow_ids=[flow.uuid]) == 0
    for j in (j1, j2):
        with pytest.raises(ValueError, match=".*has no outputs.*"):
            job_controller.jobstore.get_output(j.uuid)

    # the files are not deleted, and the connection was attempted only once
    for ji in jobs_info:
        assert os.path.isdir(ji.run_dir)
    assert len(connect_calls) == 1

    # each folder that was not deleted is logged
    assert "Could not connect to the worker 'test_local_worker'" in caplog.text
    for ji in jobs_info:
        assert f"The files of Job {ji.db_id} in {ji.run_dir} were not deleted" in (
            caplog.text
        )


def test_safe_delete_files_error_on_one_job(
    job_controller, runner, caplog, monkeypatch
) -> None:
    """An error while deleting the files of a Job does not stop the others."""
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.remote.host.local import LocalHost
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    j2 = add(j1.output, 4)
    flow = Flow([j1, j2])
    submit_flow(flow, worker="test_local_worker")
    runner.run_all_jobs(max_seconds=20)

    j1_info = job_controller.get_job_info(job_id=j1.uuid, job_index=j1.index)
    j2_info = job_controller.get_job_info(job_id=j2.uuid, job_index=j2.index)

    # emulate a file system that does not answer in time for the first Job only
    orig_listdir = LocalHost.listdir

    def slow_listdir(self, path):
        if str(path) == j1_info.run_dir:
            raise TimeoutError("the command did not complete in time")
        return orig_listdir(self, path)

    monkeypatch.setattr(LocalHost, "listdir", slow_listdir)

    deleted = job_controller.safe_delete_files([j1_info, j2_info])

    assert [ji.db_id for ji in deleted] == [j2_info.db_id]
    assert os.path.isdir(j1_info.run_dir)
    assert not os.path.isdir(j2_info.run_dir)
    assert (
        f"The files of Job {j1_info.db_id} in {j1_info.run_dir} on the worker "
        "'test_local_worker' were not deleted"
    ) in caplog.text
    assert "the command did not complete in time" in caplog.text


def test_complete_onmissing_none(job_controller, runner):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import always_fails, onmissing_none

    j1 = always_fails()
    j2 = onmissing_none(j1.output)
    flow = Flow([j1, j2])

    submit_flow(flow, worker="test_local_worker")

    runner.run_one_job(max_seconds=20)

    assert job_controller.get_job_info(job_id=j1.uuid).state == JobState.FAILED
    assert job_controller.get_job_info(job_id=j2.uuid).state == JobState.READY

    runner.run_one_job(max_seconds=20)

    assert job_controller.get_job_info(job_id=j2.uuid).state == JobState.COMPLETED
    # The job returns the input argument, so it should be None
    assert job_controller.get_job_output(job_id=j2.uuid) is None


def test_batches(job_controller):
    assert job_controller.add_batch_process(
        process_id="1234", batch_uid="uuid1", worker="test_local_batch_worker"
    )
    assert job_controller.add_batch_process(
        process_id="1235", batch_uid="uuid2", worker="test_local_batch_worker"
    )

    assert job_controller.count_batches() == 2

    job_controller.update_job_in_batch(
        job_id="job_uuid1", job_index=1, db_id="1", batch_uid="uuid1"
    )
    job_controller.update_job_in_batch(
        job_id="job_uuid2", job_index=1, db_id="2", batch_uid="uuid1"
    )
    # the same job updated twice should not be inserted more than once:
    job_controller.update_job_in_batch(
        job_id="job_uuid1", job_index=1, db_id="1", batch_uid="uuid1"
    )
    job_controller.update_job_in_batch(
        job_id="job_uuid3", job_index=1, db_id="3", batch_uid="uuid2"
    )
    batches = job_controller.get_batches(batch_uid="uuid1")
    assert len(batches) == 1
    assert len(batches[0].jobs) == 2

    assert job_controller.count_batches(job_ids=("job_uuid1", 1)) == 1
    assert (
        job_controller.count_batches(job_ids=[("job_uuid1", 1), ("job_uuid3", 1)]) == 2
    )
    assert job_controller.count_batches(db_ids="1") == 1
    assert job_controller.count_batches(db_ids=["1", "3"]) == 2


def test_stop_job_submitted(job_controller, runner) -> None:
    """Test stopping a job in SUBMITTED state cancels the queue process."""
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add_sleep

    job = add_sleep(1, 30)
    flow = Flow([job])
    submit_flow(flow, worker="test_local_worker")

    runner.run_one_job(max_seconds=20, target_state=JobState.RUNNING)

    qm = runner.get_queue_manager("test_local_worker")
    job_info = job_controller.get_job_info(job_id=job.uuid, job_index=job.index)
    process_id = job_info.remote.process_id

    jobs_list = qm.get_jobs_list([process_id])
    assert len(jobs_list) == 1

    # Manually set state back to SUBMITTED to test that branch
    job_controller.set_job_state(
        JobState.SUBMITTED, job_id=job.uuid, job_index=job.index
    )
    assert job_controller.get_job_info(job_id=job.uuid).state == JobState.SUBMITTED

    job_controller.stop_job(job_id=job.uuid)

    jobs_list = qm.get_jobs_list([process_id])
    assert len(jobs_list) == 0
    assert job_controller.get_job_info(job_id=job.uuid).state == JobState.USER_STOPPED
    assert job_controller.get_flows_info(job_ids=job.uuid)[0].state == FlowState.STOPPED


def test_stop_job_running(job_controller, runner) -> None:
    """Test stopping a job in RUNNING state cancels the queue process."""
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import FlowState, JobState
    from jobflow_remote.testing import add_sleep

    job = add_sleep(1, 30)
    flow = Flow([job])
    submit_flow(flow, worker="test_local_worker")

    runner.run_one_job(max_seconds=20, target_state=JobState.RUNNING)

    qm = runner.get_queue_manager("test_local_worker")
    job_info = job_controller.get_job_info(job_id=job.uuid, job_index=job.index)
    process_id = job_info.remote.process_id

    assert job_info.state == JobState.RUNNING
    jobs_list = qm.get_jobs_list([process_id])
    assert len(jobs_list) == 1

    job_controller.stop_job(job_id=job.uuid)

    jobs_list = qm.get_jobs_list([process_id])
    assert len(jobs_list) == 0
    assert job_controller.get_job_info(job_id=job.uuid).state == JobState.USER_STOPPED
    assert job_controller.get_flows_info(job_ids=job.uuid)[0].state == FlowState.STOPPED


def test_collect_cancel_errors(job_controller) -> None:
    """
    A failed cancellation is collected with the db_id of the Job, and the
    action is performed anyway.
    """
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.jobcontroller import collect_cancel_errors
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    j2 = add(j1.output, 3)
    j3 = add(j1.output, 4)
    flow = Flow([j1, j2, j3])
    submit_flow(flow, worker="test_local_worker")
    db_ids = {j.uuid: job_controller.get_job_info(job_id=j.uuid).db_id for j in flow}

    def set_state(job, state=JobState.RUNNING) -> None:
        # a RUNNING Job without a process id: its cancellation fails
        assert job_controller.set_job_doc_properties(
            {"state": state.value}, job_id=job.uuid
        )

    def state(job) -> JobState:
        return job_controller.get_job_info(job_id=job.uuid).state

    with collect_cancel_errors() as errors:
        set_state(j2)
        job_controller.stop_jobs(job_ids=[(j2.uuid, 1)])
        assert state(j2) == JobState.USER_STOPPED

        # rerun of a RUNNING Job
        set_state(j2)
        job_controller.rerun_job(job_id=j2.uuid)
        assert state(j2) == JobState.READY

        # forced rerun of the parent of a RUNNING Job
        set_state(j1, JobState.COMPLETED)
        set_state(j2)
        job_controller.rerun_job(job_id=j1.uuid, force=True)
        assert state(j2) == JobState.WAITING

        set_state(j3)
        job_controller.delete_job(job_id=j3.uuid)
        assert job_controller.count_jobs(job_ids=[(j3.uuid, 1)]) == 0

        set_state(j1)
        assert job_controller.delete_flow(flow.uuid)

    assert [db_id for db_id, _ in errors] == [
        db_ids[j2.uuid],
        db_ids[j2.uuid],
        db_ids[j2.uuid],
        db_ids[j3.uuid],
        db_ids[j1.uuid],
    ]
    for _, exc in errors:
        assert isinstance(exc, ValueError)
        assert "process id is not defined" in str(exc)


# the Jobs are independent, so that they can run at the same time
@pytest.mark.filterwarnings("ignore:Some jobs are not connected")
def test_many_jobs_action_shares_hosts(job_controller, runner, monkeypatch) -> None:
    """Cancelling the processes of several Jobs connects to their worker once."""
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.jobs.state import JobState
    from jobflow_remote.remote.host.local import LocalHost
    from jobflow_remote.testing import add_sleep

    jobs = [add_sleep(1, 30), add_sleep(2, 30)]
    flow = Flow(jobs)
    submit_flow(flow, worker="test_local_worker")
    for j in jobs:
        runner.run_one_job(
            max_seconds=20, job_id=[j.uuid, j.index], target_state=JobState.RUNNING
        )

    qm = runner.get_queue_manager("test_local_worker")
    jobs_info = [job_controller.get_job_info(job_id=j.uuid) for j in jobs]
    process_ids = [ji.remote.process_id for ji in jobs_info]
    assert {ji.state for ji in jobs_info} == {JobState.RUNNING}
    assert len(qm.get_jobs_list(process_ids)) == 2

    connect_calls = []
    orig_connect = LocalHost.connect

    def counting_connect(self) -> None:
        connect_calls.append(self)
        orig_connect(self)

    with monkeypatch.context() as m:
        m.setattr(LocalHost, "connect", counting_connect)
        updated = job_controller.stop_jobs(flow_ids=[flow.uuid])

    assert set(updated) == {ji.db_id for ji in jobs_info}
    assert len(connect_calls) == 1
    assert len(qm.get_jobs_list(process_ids)) == 0
    for j in jobs:
        assert job_controller.get_job_info(job_id=j.uuid).state == JobState.USER_STOPPED
