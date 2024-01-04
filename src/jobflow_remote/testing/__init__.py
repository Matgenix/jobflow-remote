"""A series of toy workflows that can be used for testing."""
from jobflow import job


@job
def add(a, b):
    """Adds two numbers together and writes the answer to a file."""
    return a + b


@job
def always_fails():
    """A job that always fails."""
    raise RuntimeError("This job failed.")


@job
def write_file(n):
    with open("results.txt", "w") as f:
        f.write(str(n))
    return
