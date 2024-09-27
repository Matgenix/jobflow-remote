def test_set_run_config(job_controller):
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
