import sys

import pytest

NAMES = [
    "relax",
    "relax_1",
    "relax_2",
    "relaxation",
    "pre_relax",
    "relax.x-1",
    "relax.x-2",
    "relaxyx-1",
    "rel+ax",
    "re(l)ax",
    "rel-ax",
    "relbx",
    "static",
]

PATTERNS = [
    "relax",
    "relax*",
    "*relax*",
    "*relax",
    "rel?x",
    "rel[ab]x",
    "rel[!a]x",
    "relax.x*",
    "relax.x-*",
    "relax.*",
    "rel+ax",
    "re(l)ax",
    "re(l*",
    "rel-ax",
    "r*x*1",
    "abc[def",
    "missing",
]


def test_fnmatch_to_mongo_query_form():
    import fnmatch

    from jobflow_remote.utils.db import fnmatch_to_mongo_query

    # no wildcard: equality
    assert fnmatch_to_mongo_query("relax") == "relax"
    assert fnmatch_to_mongo_query("relax.x-1") == "relax.x-1"

    # literal prefix outside of the group, with the regex characters escaped
    assert fnmatch_to_mongo_query("relax*") == {
        "$regex": "^relax" + fnmatch.translate("*")
    }
    assert fnmatch_to_mongo_query("relax.x-*") == {
        "$regex": "^relax\\.x\\-" + fnmatch.translate("*")
    }
    assert fnmatch_to_mongo_query("rel[ab]x") == {
        "$regex": "^rel" + fnmatch.translate("[ab]x")
    }


# TODO remove when dropping support for python 3.10
@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="fnmatch.translate in python 3.10 generates group names from a global "
    "counter, so two translations of the same pattern differ",
)
def test_fnmatch_to_mongo_query_leading_wildcard():
    import fnmatch

    from jobflow_remote.utils.db import fnmatch_to_mongo_query

    # a leading wildcard or a backslash keep the full translation
    for pattern in ("*relax*", "?elax", "[rs]elax", "rel\\ax*"):
        expected = "^" + fnmatch.translate(pattern).replace("\\\\", "\\")
        assert fnmatch_to_mongo_query(pattern) == {"$regex": expected}


@pytest.mark.parametrize("pattern", PATTERNS)
def test_fnmatch_to_mongo_query_matches(pattern):
    import fnmatch
    import re

    from jobflow_remote.utils.db import fnmatch_to_mongo_query

    query = fnmatch_to_mongo_query(pattern)
    if isinstance(query, str):
        matched = [n for n in NAMES if n == query]
    else:
        matched = [n for n in NAMES if re.search(query["$regex"], n)]

    assert matched == [n for n in NAMES if fnmatch.fnmatchcase(n, pattern)]
