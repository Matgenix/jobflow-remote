"""A series of toy workflows that can be used for testing."""
from jobflow import job


@job
def add(a, b):
    """Adds two numbers together."""
    return a + b
