"""Tests for SeparatedTransferHost."""

from unittest.mock import MagicMock, PropertyMock


def test_execute_delegates_to_command_host():
    """Test that execute() delegates to command_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    command_host.execute.return_value = ("stdout", "stderr", 0)
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    result = host.execute("ls -la")

    command_host.execute.assert_called_once()
    transfer_host.execute.assert_not_called()
    assert result == ("stdout", "stderr", 0)


def test_put_delegates_to_transfer_host():
    """Test that put() delegates to transfer_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    host.put("/local/file", "/remote/file")

    transfer_host.put.assert_called_once_with("/local/file", "/remote/file")
    command_host.put.assert_not_called()


def test_mkdir_delegates_to_transfer_host():
    """Test that mkdir() delegates to transfer_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()
    transfer_host.mkdir.return_value = True

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    result = host.mkdir("/path/to/dir")

    transfer_host.mkdir.assert_called_once()
    command_host.mkdir.assert_not_called()
    assert result is True


def test_connect_connects_both_hosts():
    """Test that connect() connects both hosts."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    host.connect()

    command_host.connect.assert_called_once()
    transfer_host.connect.assert_called_once()


def test_is_connected_requires_both_hosts():
    """Test that is_connected requires both hosts to be connected."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    type(command_host).is_connected = PropertyMock(return_value=True)
    transfer_host = MagicMock()
    type(transfer_host).is_connected = PropertyMock(return_value=False)

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)

    assert host.is_connected is False
