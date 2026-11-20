import pytest


@pytest.mark.parametrize(
    ("obj", "expected"),
    [
        (None, False),
        ([], False),
        ({"name": "relax", "state": {"$in": ["READY"]}}, False),
        ([{"$match": {"name": "x"}}, {"$sort": {"updated_on": 1}}], False),
        # similar names are not a reference
        ({"jobs_list_other": 1, "jobs": "jobs_list"}, False),
        ({"jobs_list": {"$size": 2}}, True),
        ({"jobs_list.state": "READY"}, True),
        ({"$or": [{"name": "x"}, {"jobs_list.job.name": "y"}]}, True),
        ([{"$sort": {"jobs_list.updated_on": -1}}], True),
        ({"$expr": {"$gt": [{"$size": "$jobs_list"}, 1]}}, True),
        ({"$expr": {"$in": ["READY", "$jobs_list.state"]}}, True),
        # references through the aggregation variables
        ({"$expr": {"$gt": [{"$size": "$$ROOT.jobs_list"}, 1]}}, True),
        ({"$expr": {"$in": ["READY", "$$CURRENT.jobs_list.state"]}}, True),
        ({"$expr": {"$eq": ["$$ROOT.name", "x"]}}, False),
        ({"$expr": {"$gt": [{"$size": {"$objectToArray": "$$ROOT"}}, 3]}}, True),
        # references that cannot be resolved are considered as matching
        ({"$expr": {"$gt": [{"$size": {"$getField": "jobs_list"}}, 1]}}, True),
        ({"$where": "this.jobs_list.length > 1"}, True),
        ({"$expr": {"$function": {"body": "f", "args": [], "lang": "js"}}}, True),
    ],
)
def test_references_field(obj, expected):
    from jobflow_remote.jobs.jobcontroller import _references_field

    assert _references_field(obj, "jobs_list") is expected
