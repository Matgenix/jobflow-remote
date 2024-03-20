def test_version():
    from jobflow_remote import __version__

    assert __version__.startswith("0.0.1")


def test_imports():
    """This test triggers all the top-level imports by importing
    the global `SETTINGS`.

    """
    from jobflow_remote import SETTINGS  # noqa

    ...
