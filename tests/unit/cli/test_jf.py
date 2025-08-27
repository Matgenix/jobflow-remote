import pytest


@pytest.fixture()
def reset_additional_loggers():
    from jobflow_remote.cli import jf

    backup = list(jf.ADDITIONAL_LOGGERS)
    yield
    jf.ADDITIONAL_LOGGERS = backup


def test_add_cli_logger_names(reset_additional_loggers):
    from jobflow_remote.cli.jf import ADDITIONAL_LOGGERS, add_cli_logger_names

    assert [] == ADDITIONAL_LOGGERS

    add_cli_logger_names("test_test")

    assert ["test_test"] == ADDITIONAL_LOGGERS

    add_cli_logger_names(["test1", "test2"])

    assert ["test_test", "test1", "test2"] == ADDITIONAL_LOGGERS
