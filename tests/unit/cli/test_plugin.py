import types
from unittest.mock import MagicMock, Mock


def test_mock_load_plugins_with_valid_plugin(mocker):
    """Test that a valid plugin with setup function is loaded and executed."""
    from jobflow_remote.cli.plugin import PLUGIN_GROUP, load_plugins
    from jobflow_remote.testing.cli import run_check_cli

    # Mock the plugin. Nothing is really added to the CLI
    mock_entry_point = Mock()
    mock_entry_point.name = "test_plugin"

    mock_plugin_module = Mock()
    mock_setup_function = Mock()
    mock_plugin_module.setup_jf_plugin = mock_setup_function
    mock_entry_point.load.return_value = mock_plugin_module

    mock_entry_points = Mock()
    mock_entry_points.select.return_value = [mock_entry_point]
    mocker.patch("importlib.metadata.entry_points", return_value=mock_entry_points)

    load_plugins()

    mock_entry_points.select.assert_called_once_with(group=PLUGIN_GROUP)
    mock_entry_point.load.assert_called_once()
    mock_setup_function.assert_called_once()

    run_check_cli(
        ["plugin", "list"], required_out=["Available plugins:", "test_plugin"]
    )


def test_mock_load_plugins_with_multiple_valid_plugins(mocker):
    """Test loading multiple plugins successfully."""
    from jobflow_remote.cli.plugin import PLUGIN_GROUP, load_plugins
    from jobflow_remote.testing.cli import run_check_cli

    # Mock multiple plugins. Nothing is really added to the CLI
    mock_entry_points_list = []
    mock_setup_functions = []

    for i in range(3):
        mock_entry_point = Mock()
        mock_entry_point.name = f"test_plugin_{i}"

        mock_plugin_module = Mock()
        mock_setup_function = Mock()
        mock_plugin_module.setup_jf_plugin = mock_setup_function
        mock_entry_point.load.return_value = mock_plugin_module

        mock_entry_points_list.append(mock_entry_point)
        mock_setup_functions.append(mock_setup_function)

    mock_entry_points = Mock()
    mock_entry_points.select.return_value = mock_entry_points_list
    mocker.patch("importlib.metadata.entry_points", return_value=mock_entry_points)

    load_plugins()

    mock_entry_points.select.assert_called_once_with(group=PLUGIN_GROUP)
    assert all(ep.load.call_count == 1 for ep in mock_entry_points_list)
    assert all(setup_func.call_count == 1 for setup_func in mock_setup_functions)

    run_check_cli(
        ["plugin", "list"],
        required_out=[
            "Available plugins:",
            "test_plugin_0",
            "test_plugin_1",
            "test_plugin_2",
        ],
    )


def test_mock_load_plugins_with_plugin_missing_setup_function(mocker):
    """Test a plugin where the setup function is missing."""
    from jobflow_remote.cli.plugin import (
        PLUGIN_GROUP,
        PLUGIN_LOAD_FUNCTION,
        load_plugins,
    )
    from jobflow_remote.testing.cli import run_check_cli

    # Mock the plugin.
    mock_entry_point = Mock()
    mock_entry_point.name = "incomplete_plugin"

    # Create a fake module
    fake_module = types.ModuleType("fake_module")

    # Add a mocked function, but not the setup_jf_plugin
    fake_module.some_function = MagicMock(return_value="mocked")
    assert not hasattr(fake_module, PLUGIN_LOAD_FUNCTION)

    mock_entry_point.load.return_value = fake_module

    mock_entry_points = Mock()
    mock_entry_points.select.return_value = [mock_entry_point]
    mocker.patch("importlib.metadata.entry_points", return_value=mock_entry_points)

    mock_logger = mocker.patch("logging.debug")

    load_plugins()

    mock_entry_points.select.assert_called_once_with(group=PLUGIN_GROUP)
    mock_entry_point.load.assert_called_once()

    mock_logger.assert_not_called()

    run_check_cli(
        ["plugin", "list"],
        required_out="No plugins found.",
        excluded_out="Available plugins:",
    )

    run_check_cli(
        ["plugin", "list", "-e"],
        required_out=[
            "No plugins found.",
            "Errors discovering plugins",
            "incomplete_plugin",
            "No setup_jf_plugin function",
        ],
        excluded_out=["Available plugins:"],
    )


def test_mock_load_plugins_with_plugin_load_failure(mocker):
    """Test that plugin load failure is handled."""
    from jobflow_remote.cli.plugin import load_plugins
    from jobflow_remote.testing.cli import run_check_cli

    # Mock entry point that raises exception on load
    mock_entry_point = Mock()
    mock_entry_point.name = "broken_plugin"
    mock_entry_point.load.side_effect = ImportError("Plugin module not found")

    mock_entry_points = Mock()
    mock_entry_points.select.return_value = [mock_entry_point]
    mocker.patch("importlib.metadata.entry_points", return_value=mock_entry_points)

    mock_logger = mocker.patch("logging.debug")

    load_plugins()

    mock_logger.assert_called()
    logged_calls = [
        call
        for call in mock_logger.call_args_list
        if "Failed to load plugin" in str(call)
    ]
    assert len(logged_calls) > 0

    run_check_cli(
        ["plugin", "list"],
        required_out="No plugins found.",
        excluded_out="Available plugins:",
    )

    run_check_cli(
        ["plugin", "list", "-e"],
        required_out=[
            "No plugins found.",
            "Errors discovering plugins",
            "Plugin module not found",
        ],
        excluded_out=["Available plugins:"],
    )


def test_mock_load_plugins_with_setup_function_failure(mocker):
    """Test that setup function failure is handled."""
    from jobflow_remote.cli.plugin import load_plugins
    from jobflow_remote.testing.cli import run_check_cli

    # Mock entry point
    mock_entry_point = Mock()
    mock_entry_point.name = "failing_setup_plugin"

    mock_plugin_module = Mock()
    mock_setup_function = Mock()
    mock_setup_function.side_effect = RuntimeError("Setup failed")
    mock_plugin_module.setup_jf_plugin = mock_setup_function
    mock_entry_point.load.return_value = mock_plugin_module

    mock_entry_points = Mock()
    mock_entry_points.select.return_value = [mock_entry_point]
    mocker.patch("importlib.metadata.entry_points", return_value=mock_entry_points)

    mock_logger = mocker.patch("logging.debug")

    load_plugins()

    mock_setup_function.assert_called_once()
    mock_logger.assert_called()
    logged_calls = [
        call
        for call in mock_logger.call_args_list
        if "Failed to load plugin" in str(call)
    ]
    assert len(logged_calls) > 0

    run_check_cli(
        ["plugin", "list"],
        required_out="No plugins found.",
        excluded_out="Available plugins:",
    )

    run_check_cli(
        ["plugin", "list", "-e"],
        required_out=[
            "No plugins found.",
            "Errors discovering plugins",
            "Setup failed",
        ],
        excluded_out=["Available plugins:"],
    )


def test_mock_load_plugins_with_no_plugins(mocker):
    """Test behavior when no plugins are discovered."""
    from jobflow_remote.cli.plugin import PLUGIN_GROUP, load_plugins
    from jobflow_remote.testing.cli import run_check_cli

    mock_entry_points = Mock()
    mock_entry_points.select.return_value = []
    mocker.patch("importlib.metadata.entry_points", return_value=mock_entry_points)

    load_plugins()

    mock_entry_points.select.assert_called_once_with(group=PLUGIN_GROUP)

    run_check_cli(
        ["plugin", "list"],
        required_out="No plugins found.",
        excluded_out=["Available plugins:", "Errors discovering plugins"],
    )

    run_check_cli(
        ["plugin", "list", "-e"],
        required_out="No plugins found.",
        excluded_out=["Available plugins:", "Errors discovering plugins"],
    )


def test_mock_load_plugins_with_entry_points_discovery_failure(mocker):
    """Test that entry points discovery failure is handled gracefully."""
    from jobflow_remote.cli.plugin import load_plugins
    from jobflow_remote.testing.cli import run_check_cli

    # Mock entry_points() to raise exception
    mocker.patch(
        "importlib.metadata.entry_points",
        side_effect=RuntimeError("Entry points error"),
    )

    mock_logger = mocker.patch("logging.debug")

    load_plugins()

    mock_logger.assert_called()
    logged_calls = [
        call
        for call in mock_logger.call_args_list
        if "Error discovering plugins" in str(call)
    ]
    assert len(logged_calls) > 0

    run_check_cli(
        ["plugin", "list"],
        error=True,
        required_out="Entry points error",
        excluded_out="Available plugins:",
    )


def test_mock_load_plugins_mixed_success_and_failure(mocker):
    """Test loading plugins where some succeed and some fail."""
    from jobflow_remote.cli.plugin import load_plugins
    from jobflow_remote.testing.cli import run_check_cli

    # Mixed scenario: 1 successful, 1 failing load, 1 failing setup
    mock_entry_points_list = []

    # Successful plugin
    mock_entry_point_1 = Mock()
    mock_entry_point_1.name = "good_plugin"
    mock_plugin_module_1 = Mock()
    mock_setup_function_1 = Mock()
    mock_plugin_module_1.setup_jf_plugin = mock_setup_function_1
    mock_entry_point_1.load.return_value = mock_plugin_module_1
    mock_entry_points_list.append(mock_entry_point_1)

    # Plugin that fails to load
    mock_entry_point_2 = Mock()
    mock_entry_point_2.name = "bad_load_plugin"
    mock_entry_point_2.load.side_effect = ImportError("Load failed")
    mock_entry_points_list.append(mock_entry_point_2)

    # Plugin with failing setup
    mock_entry_point_3 = Mock()
    mock_entry_point_3.name = "bad_setup_plugin"
    mock_plugin_module_3 = Mock()
    mock_setup_function_3 = Mock()
    mock_setup_function_3.side_effect = RuntimeError("Setup failed")
    mock_plugin_module_3.setup_jf_plugin = mock_setup_function_3
    mock_entry_point_3.load.return_value = mock_plugin_module_3
    mock_entry_points_list.append(mock_entry_point_3)

    # Mock entry points discovery
    mock_entry_points = Mock()
    mock_entry_points.select.return_value = mock_entry_points_list
    mocker.patch("importlib.metadata.entry_points", return_value=mock_entry_points)

    # Mock logging
    mock_logger = mocker.patch("logging.debug")

    # Execute
    load_plugins()

    # Verify successful plugin was set up
    mock_setup_function_1.assert_called_once()
    mock_setup_function_3.assert_called_once()

    # Verify errors were logged (should be 2 failed plugins)
    logged_calls = [
        call
        for call in mock_logger.call_args_list
        if "Failed to load plugin" in str(call)
    ]
    assert len(logged_calls) == 2

    run_check_cli(
        ["plugin", "list"],
        required_out=["Available plugins:", "good_plugin"],
        excluded_out=["No plugins found.", "bad_load_plugin", "bad_setup_plugin"],
    )

    run_check_cli(
        ["plugin", "list", "-e"],
        required_out=[
            "Available plugins:",
            "good_plugin",
            "bad_load_plugin",
            "bad_setup_plugin",
            "Load failed",
            "Setup failed",
        ],
        excluded_out=["No plugins found."],
    )


def test_plugin_command_injection(mocker, remove_jfremote_modules):
    """
    Test that simulates a plugin actually injecting commands into different
    points of the app.
    """
    from jobflow_remote.cli.jfr_typer import JFRTyper
    from jobflow_remote.cli.plugin import load_plugins
    from jobflow_remote.cli.utils import out_console
    from jobflow_remote.testing.cli import run_check_cli

    app_plugin_example = JFRTyper(name="pluginexample")

    @app_plugin_example.command()
    def testexample(s: str):
        out_console.print(s)

    # Create a mock plugin that adds a command to the sub_app
    def setup_jf_plugin():
        from jobflow_remote.cli.jf import app
        from jobflow_remote.cli.job import app_job

        app.add_typer(app_plugin_example)

        @app_job.command()
        def subcommandexample(i: int):
            out_console.print(i)

    mock_entry_point = Mock()
    mock_entry_point.name = "command_injection_plugin"
    mock_plugin_module = Mock()
    mock_plugin_module.setup_jf_plugin = setup_jf_plugin
    mock_entry_point.load.return_value = mock_plugin_module

    # Mock entry points discovery
    mock_entry_points = Mock()
    mock_entry_points.select.return_value = [mock_entry_point]
    mocker.patch("importlib.metadata.entry_points", return_value=mock_entry_points)

    # Execute plugin loading
    load_plugins()

    run_check_cli(
        ["-h"],
        required_out="pluginexample",
    )

    run_check_cli(
        ["pluginexample", "testexample", "inputstring"],
        required_out="inputstring",
    )

    run_check_cli(
        ["job", "-h"],
        required_out="subcommandexample",
    )

    run_check_cli(
        ["job", "subcommandexample", "100000"],
        required_out="100000",
    )

    run_check_cli(
        ["plugin", "list"],
        required_out=["Available plugins:", "command_injection_plugin"],
    )
