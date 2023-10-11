from jobflow_remote import __version__


def test_version():
    assert __version__ == "0.0.1"


def test_imports():
    """This test triggers all the top-level imports by importing
    the global `SETTINGS`.

    """
    from jobflow_remote import SETTINGS  # noqa

    ...
