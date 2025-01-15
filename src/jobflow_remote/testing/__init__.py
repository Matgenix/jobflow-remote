"""A series of toy workflows that can be used for testing."""

from typing import Callable, NoReturn, Optional, Union

from jobflow import Job, Response, job


@job
def add(a, b):
    """Adds two numbers together and writes the answer to a file."""
    return a + b


@job
def always_fails() -> NoReturn:
    """A job that always fails."""
    raise RuntimeError("This job failed.")


@job
def write_file(n) -> None:
    with open("results.txt", "w") as f:
        f.write(str(n))


@job
def arithmetic(
    a: Union[float, list[float]],
    b: Union[float, list[float]],
    op: Optional[Callable] = None,
) -> Optional[float]:
    if op:
        return op(a, b)

    return None


@job
def check_env_var() -> str:
    import os

    return os.environ.get("TESTING_ENV_VAR", "unset")


@job(big_data="data")
def add_big(a: float, b: float):
    """Adds two numbers together and inflates the answer
    to a large list and tries to store that within
    the defined store.
    """
    result = a + b
    big_array = [result] * 5_000
    return Response({"data": big_array, "result": a + b})


@job(undefined_store="data")
def add_big_undefined_store(a: float, b: float):
    """Adds two numbers together and writes the answer to an artificially large file
    which is attempted to be stored in a undefined store.
    """
    result = a + b
    return Response({"data": [result] * 5_000, "result": result})


@job
def add_sleep(a, b):
    """Adds two numbers together and sleeps for "b" seconds."""
    import time

    time.sleep(b)
    return a + b


@job
def create_detour(detour_job: Job):
    """Create a detour based on the passed Job."""
    from jobflow import Flow

    return Response(detour=Flow(detour_job))


@job
def self_replace(n: int):
    """Create a replace Job with the same job n times."""
    if n > 0:
        return Response(replace=self_replace(n - 1))

    return n


@job
def ignore_input(a: int) -> int:
    """
    Can receive an input, but ignores it.

    Allows to test flows with failed parents
    """
    return 1


@job
def current_jobdoc():
    from jobflow_remote.jobs.run import CURRENT_JOBDOC

    return CURRENT_JOBDOC.job_doc
