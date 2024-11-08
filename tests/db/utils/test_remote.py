def test_share_hosts(mocker):
    import jobflow_remote
    from jobflow_remote.utils.remote import SharedHosts

    mocker.patch("jobflow_remote.remote.host.local.LocalHost.close")

    with SharedHosts() as shared_hosts1:
        host1 = shared_hosts1.get_host("test_local_worker")
        assert len(shared_hosts1._hosts) == 1
        with SharedHosts() as shared_hosts2:
            assert len(shared_hosts2._hosts) == 1
            host2 = shared_hosts2.get_host("test_local_worker")
            # hosts should be the same instance
            assert shared_hosts1 is shared_hosts2
            assert host1 is host2

        # after leaving the first context manager the close should not be called
        jobflow_remote.remote.host.local.LocalHost.close.assert_not_called()

    # close() called only once after leaving the outermost context manager
    jobflow_remote.remote.host.local.LocalHost.close.assert_called_once()
