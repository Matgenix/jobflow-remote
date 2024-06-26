# Changelog

## [Unreleased](https://github.com/Matgenix/jobflow-remote/tree/HEAD)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v0.1.2...HEAD)

**Closed issues:**

- Feature request: support psutil 6.0.0 [\#128](https://github.com/Matgenix/jobflow-remote/issues/128)

## [v0.1.2](https://github.com/Matgenix/jobflow-remote/tree/v0.1.2) (2024-06-26)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v0.1.1...v0.1.2)

**Implemented enhancements:**

- Allow setting resources for FAILED and COMPLETED jobs [\#119](https://github.com/Matgenix/jobflow-remote/issues/119)
- SUGGESTION: Catchall state for errors [\#111](https://github.com/Matgenix/jobflow-remote/issues/111)
- Flow information [\#19](https://github.com/Matgenix/jobflow-remote/issues/19)

**Fixed bugs:**

- Flow state stays COMPLETED when a job is rerun [\#118](https://github.com/Matgenix/jobflow-remote/issues/118)

**Closed issues:**

- Feature request: show more information about the flow\(s\) that will be deleted [\#130](https://github.com/Matgenix/jobflow-remote/issues/130)
- Feature request: optionally delete outputs when doing jf flow delete [\#129](https://github.com/Matgenix/jobflow-remote/issues/129)
- SUGGESTION: Add an example for querying results from the database [\#115](https://github.com/Matgenix/jobflow-remote/issues/115)

**Merged pull requests:**

- Small updates to JobController and CLI [\#132](https://github.com/Matgenix/jobflow-remote/pull/132) ([gpetretto](https://github.com/gpetretto))
- Migrate linting from `flake8`, `isort`, `autoflake` to `ruff` [\#122](https://github.com/Matgenix/jobflow-remote/pull/122) ([janosh](https://github.com/janosh))
- Minor updates [\#121](https://github.com/Matgenix/jobflow-remote/pull/121) ([gpetretto](https://github.com/gpetretto))
- Better error message on missing project name [\#120](https://github.com/Matgenix/jobflow-remote/pull/120) ([janosh](https://github.com/janosh))

## [v0.1.1](https://github.com/Matgenix/jobflow-remote/tree/v0.1.1) (2024-03-20)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v0.1.0...v0.1.1)

**Closed issues:**

- Two projects, job submission [\#89](https://github.com/Matgenix/jobflow-remote/issues/89)
- Add guard for missing `additional_store` before executing job [\#76](https://github.com/Matgenix/jobflow-remote/issues/76)
- PyPI release checklist [\#53](https://github.com/Matgenix/jobflow-remote/issues/53)

**Merged pull requests:**

- Tests and updates [\#92](https://github.com/Matgenix/jobflow-remote/pull/92) ([gpetretto](https://github.com/gpetretto))
- Add options for remote JobStore [\#88](https://github.com/Matgenix/jobflow-remote/pull/88) ([gpetretto](https://github.com/gpetretto))
- Updates [\#87](https://github.com/Matgenix/jobflow-remote/pull/87) ([gpetretto](https://github.com/gpetretto))
- Interactive login for MFA [\#83](https://github.com/Matgenix/jobflow-remote/pull/83) ([gpetretto](https://github.com/gpetretto))
- Add testing and runtime checks for additional stores [\#59](https://github.com/Matgenix/jobflow-remote/pull/59) ([ml-evs](https://github.com/ml-evs))
- Remove errant `__init__.py` that prevents mypy from working [\#54](https://github.com/Matgenix/jobflow-remote/pull/54) ([ml-evs](https://github.com/ml-evs))

## [v0.1.0](https://github.com/Matgenix/jobflow-remote/tree/v0.1.0) (2024-02-05)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/5cdc90eea80bada88c0b565fcc0bc4c70574f5ec...v0.1.0)

**Closed issues:**

- jf job list shows errors randomly [\#63](https://github.com/Matgenix/jobflow-remote/issues/63)
- Multiple lines to `pre_run` [\#61](https://github.com/Matgenix/jobflow-remote/issues/61)
- Add tests for `Job`s with a function arg or kwarg [\#45](https://github.com/Matgenix/jobflow-remote/issues/45)
- Question about supported data types [\#44](https://github.com/Matgenix/jobflow-remote/issues/44)
- Error during retry [\#40](https://github.com/Matgenix/jobflow-remote/issues/40)
- 'REMOTE\_ERROR' state for running job [\#36](https://github.com/Matgenix/jobflow-remote/issues/36)
- Targeted Python versions [\#33](https://github.com/Matgenix/jobflow-remote/issues/33)
- Jobflow dependency [\#30](https://github.com/Matgenix/jobflow-remote/issues/30)
- remote\_job\_data.json file missing [\#26](https://github.com/Matgenix/jobflow-remote/issues/26)
- Suggestion: improve -sdate option [\#24](https://github.com/Matgenix/jobflow-remote/issues/24)
- Job info for remote failures [\#20](https://github.com/Matgenix/jobflow-remote/issues/20)
- List of flows by state does not work [\#16](https://github.com/Matgenix/jobflow-remote/issues/16)
- Runner fails when default `resources` are provider per worker [\#14](https://github.com/Matgenix/jobflow-remote/issues/14)
- Cryptic failures when worker's `workdir` is missing [\#13](https://github.com/Matgenix/jobflow-remote/issues/13)
- Worker can be set to `None` leaving dead jobs [\#11](https://github.com/Matgenix/jobflow-remote/issues/11)
- Jobs with RESERVED state have datetime objects for `last_updated` field [\#8](https://github.com/Matgenix/jobflow-remote/issues/8)
- Command line "jf project" when there is no project [\#5](https://github.com/Matgenix/jobflow-remote/issues/5)
- .jfremote yaml test unsensitive to port change [\#4](https://github.com/Matgenix/jobflow-remote/issues/4)
- config issue [\#3](https://github.com/Matgenix/jobflow-remote/issues/3)

**Merged pull requests:**

- Update release workflow and README [\#72](https://github.com/Matgenix/jobflow-remote/pull/72) ([ml-evs](https://github.com/ml-evs))
- Add dev setup docs [\#57](https://github.com/Matgenix/jobflow-remote/pull/57) ([ml-evs](https://github.com/ml-evs))
- Add a test that runs with a given `exec_config` [\#56](https://github.com/Matgenix/jobflow-remote/pull/56) ([ml-evs](https://github.com/ml-evs))
- Pin dependency versions for testing, add PyPI release and enable dependabot [\#55](https://github.com/Matgenix/jobflow-remote/pull/55) ([ml-evs](https://github.com/ml-evs))
- Linting and pre-commit updates [\#52](https://github.com/Matgenix/jobflow-remote/pull/52) ([ml-evs](https://github.com/ml-evs))
- Add integration test for job with a callable as a kwarg [\#51](https://github.com/Matgenix/jobflow-remote/pull/51) ([ml-evs](https://github.com/ml-evs))
- Add codecov upload [\#49](https://github.com/Matgenix/jobflow-remote/pull/49) ([ml-evs](https://github.com/ml-evs))
- Fix README badge [\#48](https://github.com/Matgenix/jobflow-remote/pull/48) ([ml-evs](https://github.com/ml-evs))
- Breaking changes: db\_id type, cancelled state, deserialization and documentation [\#47](https://github.com/Matgenix/jobflow-remote/pull/47) ([gpetretto](https://github.com/gpetretto))
- Added GitHub release workflow and docs builds [\#46](https://github.com/Matgenix/jobflow-remote/pull/46) ([davidwaroquiers](https://github.com/davidwaroquiers))
- \[WIP\] New job management system [\#37](https://github.com/Matgenix/jobflow-remote/pull/37) ([gpetretto](https://github.com/gpetretto))
- Add index to the folder name [\#35](https://github.com/Matgenix/jobflow-remote/pull/35) ([gpetretto](https://github.com/gpetretto))
- Linting fixes and CI config for multiple Python 3.9+ [\#34](https://github.com/Matgenix/jobflow-remote/pull/34) ([ml-evs](https://github.com/ml-evs))
- Add containerized integration tests for remote and local workers using slurm [\#32](https://github.com/Matgenix/jobflow-remote/pull/32) ([ml-evs](https://github.com/ml-evs))
- Use the qtoolkit and jobflow PyPI packages, add direct pydantic dep [\#31](https://github.com/Matgenix/jobflow-remote/pull/31) ([ml-evs](https://github.com/ml-evs))
- \[WIP\] pydantic2 updates [\#29](https://github.com/Matgenix/jobflow-remote/pull/29) ([gpetretto](https://github.com/gpetretto))
- Run CI on PRs to develop branch [\#28](https://github.com/Matgenix/jobflow-remote/pull/28) ([ml-evs](https://github.com/ml-evs))
- fix typo in cli help message [\#25](https://github.com/Matgenix/jobflow-remote/pull/25) ([FabiPi3](https://github.com/FabiPi3))
- jf flow info and other cli updates [\#23](https://github.com/Matgenix/jobflow-remote/pull/23) ([gpetretto](https://github.com/gpetretto))
- Stopped states. [\#22](https://github.com/Matgenix/jobflow-remote/pull/22) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Fixed jf flow list. [\#18](https://github.com/Matgenix/jobflow-remote/pull/18) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Restore queries based on job id [\#17](https://github.com/Matgenix/jobflow-remote/pull/17) ([gpetretto](https://github.com/gpetretto))
- Check `work_dir` with project check CLI and enforce absolute paths [\#15](https://github.com/Matgenix/jobflow-remote/pull/15) ([ml-evs](https://github.com/ml-evs))
- Fix handling of null worker in `submit_flow` [\#12](https://github.com/Matgenix/jobflow-remote/pull/12) ([ml-evs](https://github.com/ml-evs))
- Added documentation structure. [\#9](https://github.com/Matgenix/jobflow-remote/pull/9) ([davidwaroquiers](https://github.com/davidwaroquiers))
- A few minor tweaks from first use [\#7](https://github.com/Matgenix/jobflow-remote/pull/7) ([ml-evs](https://github.com/ml-evs))
- Added dependencies in pyproject.toml. [\#2](https://github.com/Matgenix/jobflow-remote/pull/2) ([davidwaroquiers](https://github.com/davidwaroquiers))
- WIP Config [\#1](https://github.com/Matgenix/jobflow-remote/pull/1) ([davidwaroquiers](https://github.com/davidwaroquiers))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
