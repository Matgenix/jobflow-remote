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


def test_mkdir_delegates_to_command_host():
    """Test that mkdir() delegates to command_host (uses SSH, not SFTP)."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    command_host.mkdir.return_value = True
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    result = host.mkdir("/path/to/dir")

    command_host.mkdir.assert_called_once()
    transfer_host.mkdir.assert_not_called()
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


def test_is_connected_both_connected():
    """Test that is_connected returns True when both hosts are connected."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    type(command_host).is_connected = PropertyMock(return_value=True)
    transfer_host = MagicMock()
    type(transfer_host).is_connected = PropertyMock(return_value=True)

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)

    assert host.is_connected is True


def test_shell_delegates_to_command_host():
    """Test that shell() delegates to command_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    host.shell(pre_cmd="source ~/.bashrc", shell="zsh")

    command_host.shell.assert_called_once_with("source ~/.bashrc", "zsh")
    transfer_host.shell.assert_not_called()


def test_copy_delegates_to_command_host():
    """Test that copy() delegates to command_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    host.copy("/remote/src", "/remote/dst")

    command_host.copy.assert_called_once_with("/remote/src", "/remote/dst")
    transfer_host.copy.assert_not_called()


def test_move_delegates_to_command_host():
    """Test that move() delegates to command_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    host.move("/remote/src", "/remote/dst")

    command_host.move.assert_called_once_with("/remote/src", "/remote/dst")
    transfer_host.move.assert_not_called()


def test_rmtree_delegates_to_command_host():
    """Test that rmtree() delegates to command_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    command_host.rmtree.return_value = True
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    result = host.rmtree("/remote/dir", raise_on_error=True)

    command_host.rmtree.assert_called_once_with("/remote/dir", True)
    transfer_host.rmtree.assert_not_called()
    assert result is True


def test_write_text_file_delegates_to_transfer_host():
    """Test that write_text_file() delegates to transfer_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    host.write_text_file("/remote/file.txt", "content")

    transfer_host.write_text_file.assert_called_once_with("/remote/file.txt", "content")
    command_host.write_text_file.assert_not_called()


def test_read_text_file_delegates_to_transfer_host():
    """Test that read_text_file() delegates to transfer_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()
    transfer_host.read_text_file.return_value = "file content"

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    result = host.read_text_file("/remote/file.txt")

    transfer_host.read_text_file.assert_called_once_with("/remote/file.txt")
    command_host.read_text_file.assert_not_called()
    assert result == "file content"


def test_get_delegates_to_transfer_host():
    """Test that get() delegates to transfer_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    host.get("/remote/file", "/local/file")

    transfer_host.get.assert_called_once_with("/remote/file", "/local/file")
    command_host.get.assert_not_called()


def test_listdir_delegates_to_transfer_host():
    """Test that listdir() delegates to transfer_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()
    transfer_host.listdir.return_value = ["file1.txt", "file2.txt"]

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    result = host.listdir("/remote/dir")

    transfer_host.listdir.assert_called_once_with("/remote/dir")
    command_host.listdir.assert_not_called()
    assert result == ["file1.txt", "file2.txt"]


def test_remove_delegates_to_transfer_host():
    """Test that remove() delegates to transfer_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    host.remove("/remote/file.txt")

    transfer_host.remove.assert_called_once_with("/remote/file.txt")
    command_host.remove.assert_not_called()


def test_exists_delegates_to_transfer_host():
    """Test that exists() delegates to transfer_host."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()
    transfer_host.exists.return_value = True

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    result = host.exists("/remote/path")

    transfer_host.exists.assert_called_once_with("/remote/path")
    command_host.exists.assert_not_called()
    assert result is True


def test_close_closes_both_hosts():
    """Test that close() closes both hosts and returns combined result."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    command_host.close.return_value = True
    transfer_host = MagicMock()
    transfer_host.close.return_value = True

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    result = host.close()

    command_host.close.assert_called_once()
    transfer_host.close.assert_called_once()
    assert result is True


def test_close_returns_false_if_one_fails():
    """Test that close() returns False if either host fails to close."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    command_host.close.return_value = True
    transfer_host = MagicMock()
    transfer_host.close.return_value = False

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)
    result = host.close()

    assert result is False


def test_interactive_login_either_host():
    """Test that interactive_login returns True if either host requires it."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    type(command_host).interactive_login = PropertyMock(return_value=False)
    transfer_host = MagicMock()
    type(transfer_host).interactive_login = PropertyMock(return_value=True)

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)

    assert host.interactive_login is True


def test_interactive_login_neither_host():
    """Test that interactive_login returns False if neither host requires it."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    type(command_host).interactive_login = PropertyMock(return_value=False)
    transfer_host = MagicMock()
    type(transfer_host).interactive_login = PropertyMock(return_value=False)

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)

    assert host.interactive_login is False


def test_equality_same_hosts():
    """Test that two SeparatedTransferHosts with equal hosts are equal."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host1 = MagicMock()
    command_host1.sanitize = False
    transfer_host1 = MagicMock()

    command_host2 = command_host1
    transfer_host2 = transfer_host1

    host1 = SeparatedTransferHost(
        command_host=command_host1, transfer_host=transfer_host1
    )
    host2 = SeparatedTransferHost(
        command_host=command_host2, transfer_host=transfer_host2
    )

    assert host1 == host2


def test_equality_different_hosts():
    """Test that two SeparatedTransferHosts with different hosts are not equal."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host1 = MagicMock()
    command_host1.sanitize = False
    transfer_host1 = MagicMock()

    command_host2 = MagicMock()
    command_host2.sanitize = False
    transfer_host2 = MagicMock()

    host1 = SeparatedTransferHost(
        command_host=command_host1, transfer_host=transfer_host1
    )
    host2 = SeparatedTransferHost(
        command_host=command_host2, transfer_host=transfer_host2
    )

    assert host1 != host2


def test_equality_not_separated_transfer_host():
    """Test that SeparatedTransferHost is not equal to other types."""
    from jobflow_remote.remote.host import SeparatedTransferHost

    command_host = MagicMock()
    command_host.sanitize = False
    transfer_host = MagicMock()

    host = SeparatedTransferHost(command_host=command_host, transfer_host=transfer_host)

    assert host != "not a host"
    assert host != command_host
    assert host != 123
