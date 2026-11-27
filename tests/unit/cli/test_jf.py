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


def test_lazy_config_manager(mocker, monkeypatch):
    from jobflow_remote.cli import utils

    # restore the module state at the end of the test
    monkeypatch.setattr(utils, "_config_manager_kwargs", None)
    monkeypatch.setattr(utils, "_shared_config_manager", None)
    mock_cm = mocker.patch.object(utils, "ConfigManager")
    mock_print = mocker.patch.object(utils, "print_selected_project")

    with pytest.raises(RuntimeError, match="needs to be initialized"):
        utils.get_config_manager()

    # the options are stored, but the ConfigManager is not created
    utils.initialize_config_manager(warn=True)
    mock_cm.assert_not_called()

    # created on first use, only once
    cm = utils.get_config_manager()
    assert utils.get_config_manager() is cm
    mock_cm.assert_called_once_with(warn=True)
    mock_print.assert_called_once_with(cm)

    # a new initialization discards the previous instance
    utils.initialize_config_manager(warn=False)
    utils.get_config_manager()
    assert mock_cm.call_count == 2
    mock_cm.assert_called_with(warn=False)


def test_help_does_not_load_config(mocker):
    from jobflow_remote.cli import utils
    from jobflow_remote.testing.cli import run_check_cli

    mock_cm = mocker.patch.object(utils, "ConfigManager")
    run_check_cli(["job", "list", "--help"], required_out="Usage")
    run_check_cli(["admin", "index", "--help"], required_out="Usage")
    mock_cm.assert_not_called()


@pytest.mark.parametrize("fails", [False, True], ids=["success", "failure"])
def test_shared_job_controller_closed(mocker, monkeypatch, fails):
    from jobflow_remote.cli import utils
    from jobflow_remote.testing.cli import run_check_cli

    mocker.patch.object(utils, "ConfigManager")
    mock_jc = mocker.Mock()
    # as if the shared JobController had been created by the command
    monkeypatch.setattr(utils, "_shared_job_controller", mock_jc)

    if fails:
        mocker.patch(
            "jobflow_remote.cli.admin.get_job_controller",
            side_effect=RuntimeError("test error"),
        )
        run_check_cli(["admin", "index", "list"], error=True)
    else:
        run_check_cli(["admin", "index", "--help"], required_out="Usage")

    # the connections are closed also when the command fails, and the
    # controller is not kept for the following executions in the same process
    mock_jc.close.assert_called_once()
    assert utils._shared_job_controller is None
