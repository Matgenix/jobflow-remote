import os
import time

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
    stdout, stderr, exitcode = frontend1_host.execute(
        "source /home/jobflow/.venv/bin/activate; jf project list"
    )
    assert exitcode == 0
    assert stdout == ""
    assert stderr.strip() == "No project available in /home/jobflow/.jfremote"
    assert frontend1_host.mkdir("/home/jobflow/.jfremote")
    assert frontend2_host.exists("/home/jobflow/.jfremote")

    project_yaml_str = """name: test
workers:
  slurm:
    type: remote
    host: slurm
    port: 22
    user: jobflow
    password: jobflow
    scheduler_type: slurm
    work_dir: /home/jobflow/jfr
    pre_run: source /home/jobflow/.venv/bin/activate
    timeout_execute: 60
queue:
  store:
    type: MongoStore
    host: mongo_server
    port: 27017
    database: somedb
    collection_name: jobs
exec_config: {}
jobstore:
  docs_store:
    type: MongoStore
    database: somedb
    host: mongo_server
    port: 27017
    collection_name: outputs
  additional_stores:
    data:
      type: GridFSStore
      database: db_name
      host: mongo_server
      port: 27017
      collection_name: data"""

    frontend1_host.write_text_file(
        "/home/jobflow/.jfremote/test.yaml", project_yaml_str
    )
    frontend1_host.mkdir("/home/jobflow/.jfremote/test")

    stdout, stderr, exitcode = frontend1_host.execute(
        "source /home/jobflow/.venv/bin/activate; jf project list"
    )
    assert exitcode == 0
    assert "- test" in stdout
    assert stderr == ""

    frontend1_host.execute(
        "source /home/jobflow/.venv/bin/activate; jf admin reset -y; jf runner start"
    )
    time.sleep(5)
    stdout, stderr, exitcode = frontend1_host.execute(
        "source /home/jobflow/.venv/bin/activate; jf runner status"
    )
    print("STATUS on frontend1")
    print(stdout)
    print(stderr)
    stdout, stderr, exitcode = frontend2_host.execute(
        "source /home/jobflow/.venv/bin/activate; jf runner status"
    )
    print("STATUS on frontend2")
    print(stdout)
    print(stderr)
