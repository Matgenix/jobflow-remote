import pytest


@pytest.mark.parametrize(
    "overwrite",
    [True, False],
)
def test_set_run_config(job_controller, overwrite):
    from jobflow import Flow

    from jobflow_remote import submit_flow
    from jobflow_remote.config.jobconfig import set_run_config
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    flow1 = Flow([j1])
    submit_flow(flow1, worker="test_local_worker")

    doc = job_controller.get_job_doc(job_id=j1.uuid)
    assert doc.worker == "test_local_worker"
    assert doc.priority == 0
    assert doc.exec_config is None
    assert doc.resources is None

    j2_1 = add(1, 2)
    j2_1.name = "to_modify"
    j2_2 = add(j2_1.output, 2)
    flow2 = Flow([j2_1, j2_2])

    flow2 = set_run_config(
        flow2,
        exec_config="test",
        worker="test_local_worker_2",
        priority=5,
        resources={"test_res": 1},
        name_filter="to_modify",
        overwrite=overwrite,
    )

    assert j2_1.config.manager_config["exec_config"] == "test"
    assert j2_1.config.manager_config["worker"] == "test_local_worker_2"
    assert j2_1.config.manager_config["priority"] == 5
    assert j2_1.config.manager_config["resources"] == {"test_res": 1}
    assert not j2_2.config.manager_config

    submit_flow(flow2, worker="test_local_worker")
    doc1 = job_controller.get_job_doc(job_id=j2_1.uuid)
    doc2 = job_controller.get_job_doc(job_id=j2_2.uuid)

    assert doc1.worker == "test_local_worker_2"
    assert doc1.priority == 5
    assert doc1.exec_config == "test"
    assert doc1.resources == {"test_res": 1}

    assert doc2.worker == "test_local_worker"
    assert doc2.priority == 0
    assert doc2.exec_config is None
    assert doc2.resources is None

    # test multiple changes
    j3_1 = add(1, 2)
    j3_1.name = "to_modify"
    j3_2 = add(j3_1.output, 2)
    flow3 = Flow([j3_1, j3_2])

    flow3 = set_run_config(
        flow3,
        exec_config="test",
        worker="test_local_worker_2",
        priority=5,
        resources={"test_res": 1},
        name_filter="to_modify",
        overwrite=overwrite,
    )

    assert j3_1.config.manager_config["exec_config"] == "test"
    assert j3_1.config.manager_config["worker"] == "test_local_worker_2"
    assert j3_1.config.manager_config["priority"] == 5
    assert j3_1.config.manager_config["resources"] == {"test_res": 1}
    assert not j3_2.config.manager_config

    flow3 = set_run_config(
        flow3,
        priority=10,
        resources={"test_res": 5},
        name_filter=None,
        overwrite=overwrite,
    )
    submit_flow(flow3, worker="test_local_worker")
    doc1 = job_controller.get_job_doc(job_id=j3_1.uuid)
    doc2 = job_controller.get_job_doc(job_id=j3_2.uuid)
    if overwrite:
        for j in (j3_1, j3_2):
            assert j.config.manager_config.get("exec_config") is None
            assert j.config.manager_config.get("worker") is None
            assert j.config.manager_config["priority"] == 10
            assert j.config.manager_config["resources"] == {"test_res": 5}
        for doc in (doc1, doc2):
            assert doc.worker == "test_local_worker"
            assert doc.priority == 10
            assert doc.exec_config is None
            assert doc.resources == {"test_res": 5}

    else:
        assert j3_1.config.manager_config.get("exec_config") == "test"
        assert j3_1.config.manager_config.get("worker") == "test_local_worker_2"
        assert j3_1.config.manager_config["priority"] == 10
        assert j3_1.config.manager_config["resources"] == {"test_res": 5}
        assert j3_2.config.manager_config.get("exec_config") is None
        assert j3_2.config.manager_config.get("worker") is None
        assert j3_2.config.manager_config["priority"] == 10
        assert j3_2.config.manager_config["resources"] == {"test_res": 5}

        assert doc1.worker == "test_local_worker_2"
        assert doc1.priority == 10
        assert doc1.exec_config == "test"
        assert doc1.resources == {"test_res": 5}

        assert doc2.worker == "test_local_worker"
        assert doc2.priority == 10
        assert doc2.exec_config is None
        assert doc2.resources == {"test_res": 5}
