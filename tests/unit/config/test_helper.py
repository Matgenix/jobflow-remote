from __future__ import annotations


def _queue_dict(database: str = "db", collection: str = "jobs") -> dict:
    return {
        "type": "MongoStore",
        "host": "h",
        "port": 27017,
        "database": database,
        "collection_name": collection,
    }


def _local_batch_worker(handle_dir: str) -> dict:
    return {
        "type": "local",
        "scheduler_type": "shell",
        "work_dir": "/some/test/path/w",
        "batch": {"jobs_handle_dir": handle_dir, "work_dir": "/some/test/path/batch"},
    }


def _remote_batch_worker(handle_dir: str, *, host: str) -> dict:
    return {
        "type": "remote",
        "scheduler_type": "slurm",
        "host": host,
        "user": "alice",
        "work_dir": "/scratch/work",
        "batch": {"jobs_handle_dir": handle_dir, "work_dir": "/scratch/batch"},
    }


def test_check_projects_conflicts():
    from jobflow_remote.config.base import Project
    from jobflow_remote.config.helper import check_projects_conflicts

    # A & B share a local host, queue database, and base_dir. B's
    # flows_collection also collides with A's main queue collection so the
    # cross-field check is verified.
    p_a = Project.model_validate(
        {
            "name": "A",
            "queue": {"store": _queue_dict(database="db_x", collection="shared_jobs")},
            "workers": {"w": _local_batch_worker("/some/test/path/handles")},
            "base_dir": "/shared/base",
        }
    )
    p_b = Project.model_validate(
        {
            "name": "B",
            "queue": {
                "store": _queue_dict(database="db_x", collection="jobs_b"),
                "flows_collection": "shared_jobs",
            },
            "workers": {"w": _local_batch_worker("/some/test/path/handles")},
            "base_dir": "/shared/base",
        }
    )
    # C & D have the same handle path on *different* remote hosts and
    # different queue databases, so they must not produce any conflict.
    p_c = Project.model_validate(
        {
            "name": "C",
            "queue": {"store": _queue_dict(database="db_c", collection="jobs_c")},
            "workers": {
                "w": _remote_batch_worker("/scratch/handles", host="host1.example.com")
            },
            "base_dir": "/projects/c",
        }
    )
    p_d = Project.model_validate(
        {
            "name": "D",
            "queue": {"store": _queue_dict(database="db_d", collection="jobs_d")},
            "workers": {
                "w": _remote_batch_worker("/scratch/handles", host="host2.example.com")
            },
            "base_dir": "/projects/d",
        }
    )

    issues = check_projects_conflicts({"A": p_a, "B": p_b, "C": p_c, "D": p_d})

    by_kind: dict[str, list] = {}
    for issue in issues:
        by_kind.setdefault(issue.kind, []).append(issue)

    # jobs_handle_dir: only A/B (same local host); C/D are on different
    # remote hosts and must NOT collide despite the identical path.
    handle_issues = by_kind["jobs_handle_dir"]
    assert len(handle_issues) == 1
    assert handle_issues[0].projects == ["A", "B"]
    assert "/some/test/path/handles" in handle_issues[0].message

    # Queue collections: only A/B; the cross-field hit must pair A.queue.store
    # with B.flows_collection.
    queue_issues = by_kind["queue_collection"]
    assert all(set(i.projects) == {"A", "B"} for i in queue_issues)
    assert any(
        "A.queue.store" in i.message and "B.flows_collection" in i.message
        for i in queue_issues
    )

    # Directories: A/B share base_dir, and tmp/log/daemon all default to
    # subfolders of base_dir, so we get 4 separate directory issues.
    dir_issues = by_kind["directory"]
    assert len(dir_issues) == 4
    assert all(i.projects == ["A", "B"] for i in dir_issues)

    # C and D must not appear in any issue.
    for kind_issues in by_kind.values():
        for issue in kind_issues:
            assert "C" not in issue.projects
            assert "D" not in issue.projects
