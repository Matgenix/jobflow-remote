import pytest


@pytest.fixture()
def memory_jobstore():
    from jobflow import JobStore
    from maggma.stores import MemoryStore

    store = JobStore(MemoryStore())
    store.connect()
    yield store
    store.close()


def _roundtrip(job_dict: dict):
    """Serialize and deserialize a job dict, as done when uploading to the worker."""
    import json

    from monty.json import MontyDecoder, jsanitize

    serialized = json.dumps(
        jsanitize(job_dict, strict=True, allow_bson=True, enum_values=True)
    )
    return MontyDecoder().decode(serialized)


@pytest.mark.parametrize("resolve_references", [True, False])
def test_resolve_job_dict_args_bool(memory_jobstore, resolve_references):
    from jobflow import OutputReference

    from jobflow_remote.remote.data import resolve_job_dict_args
    from jobflow_remote.testing import add

    j1 = add(1, 2)
    j2 = add(j1.output, b=j1.output)
    j2.config.resolve_references = resolve_references
    j1.run(memory_jobstore)

    job_dict = resolve_job_dict_args(j2.as_dict(), memory_jobstore)

    assert job_dict["config"]["resolve_references"] is resolve_references
    job = _roundtrip(job_dict)
    if resolve_references:
        assert list(job.function_args) == [3]
        assert job.function_kwargs == {"b": 3}
    else:
        assert isinstance(job.function_args[0], OutputReference)
        assert isinstance(job.function_kwargs["b"], OutputReference)


@pytest.mark.requires_jobflow("0.4.0")
def test_resolve_job_dict_args_both(memory_jobstore):
    from jobflow import ResolvedReference

    from jobflow_remote.remote.data import resolve_job_dict_args
    from jobflow_remote.testing import add, resolve_both

    j1 = add(1, 2)
    j2 = resolve_both([j1.output, {"a": j1.output}])
    j1.run(memory_jobstore)

    job_dict = resolve_job_dict_args(j2.as_dict(), memory_jobstore)

    # resolution should be disabled on the worker side
    assert job_dict["config"]["resolve_references"] is False
    job = _roundtrip(job_dict)
    arg_list = job.function_args[0]
    for rr in (arg_list[0], arg_list[1]["a"]):
        assert isinstance(rr, ResolvedReference)
        assert rr.uuid == j1.uuid
        assert rr.value == 3
        assert rr.reference == j1.output


@pytest.mark.requires_jobflow("0.4.0")
def test_resolve_job_dict_args_both_on_missing_pass(memory_jobstore):
    from jobflow import JobStore, OnMissing, OutputReference, ResolvedReference
    from maggma.stores import MemoryStore

    from jobflow_remote.remote.data import resolve_job_dict_args
    from jobflow_remote.testing import add, resolve_both

    j1 = add(1, 2)
    j_missing = add(1, 2)
    j2 = resolve_both(j_missing.output)
    j2.config.on_missing_references = OnMissing.PASS
    j1.run(memory_jobstore)

    job_dict = resolve_job_dict_args(j2.as_dict(), memory_jobstore)
    job = _roundtrip(job_dict)

    def check_arg(arg):
        assert isinstance(arg, ResolvedReference)
        assert arg.uuid == j_missing.uuid
        # the unresolved reference is wrapped only once
        assert isinstance(arg.value, OutputReference)
        assert arg.value == j_missing.output

    check_arg(job.function_args[0])

    # Running the job on the worker, with an empty store, should not resolve and
    # wrap the references a second time.
    worker_store = JobStore(MemoryStore())
    worker_store.connect()
    try:
        response = job.run(worker_store)
    finally:
        worker_store.close()
    check_arg(job.function_args[0])
    assert response.output["type"] == "ResolvedReference"
    assert response.output["value"] == j_missing.output
