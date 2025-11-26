import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("CI"),
    reason="Only run integration tests in CI, unless forced with 'CI' env var",
)


def test_multifrontend(frontend1_host, frontend2_host):
    frontend1_host.rmtree("/home/jobflow/jfr/*")
    frontend1_host.rmtree("/home/jobflow/.jfremote")
    assert frontend1_host.exists("/home/jobflow/jfr")
    assert frontend2_host.exists("/home/jobflow/jfr")
    assert not frontend1_host.exists("/home/jobflow/.jfremote")
    assert not frontend2_host.exists("/home/jobflow/.jfremote")
    assert frontend1_host.listdir("/home/jobflow/jfr") == []
    assert frontend2_host.listdir("/home/jobflow/jfr") == []
    assert frontend1_host.mkdir("/home/jobflow/.jfremote")
    assert frontend2_host.exists("/home/jobflow/.jfremote")
