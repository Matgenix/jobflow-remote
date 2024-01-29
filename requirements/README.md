# Strict requirements

These files contain pinned direct dependencies for use in testing, monitored by
dependabot to ensure version upgrades compatible with `pyproject.toml` do not
break the released version.

In cases where dependabot updates a requirements file and the tests no longer
pass, this may indicate that the supported versions in `pyproject.toml` need to
be upgraded (and a release made).
