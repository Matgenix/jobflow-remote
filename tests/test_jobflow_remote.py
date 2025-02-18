import pytest


@pytest.mark.unit()
def test_version() -> None:
    from jobflow_remote import __version__

    assert __version__.startswith("0."), f"unexpected {__version__=}"


@pytest.mark.unit()
def test_imports() -> None:
    """This test triggers all the top-level imports by importing
    the global `SETTINGS`.

    """
    from jobflow_remote import SETTINGS  # noqa: F401
