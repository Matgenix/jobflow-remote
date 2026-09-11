# Changelog

## [v1.0.1](https://github.com/Matgenix/jobflow-remote/tree/v1.0.1) (2026-07-23)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v1.0.0...v1.0.1)

**Closed issues:**

- wk\_name and worker\_name different [\#483](https://github.com/Matgenix/jobflow-remote/issues/483)
- New release \(\>0.1.8\) [\#426](https://github.com/Matgenix/jobflow-remote/issues/426)
- Cannot use the secondly defined local worker [\#411](https://github.com/Matgenix/jobflow-remote/issues/411)
- Remote Error while batch job is still running [\#403](https://github.com/Matgenix/jobflow-remote/issues/403)

**Merged pull requests:**

- Fix failing tests [\#509](https://github.com/Matgenix/jobflow-remote/pull/509) ([gpetretto](https://github.com/gpetretto))
- Bump actions/setup-python from 6 to 7 [\#507](https://github.com/Matgenix/jobflow-remote/pull/507) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add --latest-flow / -lf option to jf job list [\#498](https://github.com/Matgenix/jobflow-remote/pull/498) ([Luguza](https://github.com/Luguza))
- Bump actions/checkout from 6 to 7 [\#495](https://github.com/Matgenix/jobflow-remote/pull/495) ([dependabot[bot]](https://github.com/apps/dependabot))
- Several small bugfix [\#494](https://github.com/Matgenix/jobflow-remote/pull/494) ([gpetretto](https://github.com/gpetretto))
- Fix get\_flows\_info call in gui [\#491](https://github.com/Matgenix/jobflow-remote/pull/491) ([gpetretto](https://github.com/gpetretto))
- compatibility for typer dropping click dependency [\#489](https://github.com/Matgenix/jobflow-remote/pull/489) ([gpetretto](https://github.com/gpetretto))
- Improve project validation and add project cross checks [\#484](https://github.com/Matgenix/jobflow-remote/pull/484) ([gpetretto](https://github.com/gpetretto))
- Bump actions/upload-pages-artifact from 4 to 5 [\#471](https://github.com/Matgenix/jobflow-remote/pull/471) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/deploy-pages from 4 to 5 [\#467](https://github.com/Matgenix/jobflow-remote/pull/467) ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: correct enum comparison in stop\_job for SUBMITTED/RUNNING states [\#466](https://github.com/Matgenix/jobflow-remote/pull/466) ([bud-primordium](https://github.com/bud-primordium))
- Use projection in MongoLock [\#464](https://github.com/Matgenix/jobflow-remote/pull/464) ([gpetretto](https://github.com/gpetretto))
- Bump docker/bake-action from 6 to 7 [\#462](https://github.com/Matgenix/jobflow-remote/pull/462) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump docker/setup-buildx-action from 3 to 4 [\#461](https://github.com/Matgenix/jobflow-remote/pull/461) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/download-artifact from 7 to 8 [\#460](https://github.com/Matgenix/jobflow-remote/pull/460) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/upload-artifact from 6 to 7 [\#459](https://github.com/Matgenix/jobflow-remote/pull/459) ([dependabot[bot]](https://github.com/apps/dependabot))
- bump minimum supervisor requirement [\#458](https://github.com/Matgenix/jobflow-remote/pull/458) ([gpetretto](https://github.com/gpetretto))
- New SeparatedTransferHost that accommodates SFTP-disabled submission nodes [\#456](https://github.com/Matgenix/jobflow-remote/pull/456) ([mcgalcode](https://github.com/mcgalcode))
- Shutdown all runners [\#444](https://github.com/Matgenix/jobflow-remote/pull/444) ([gpetretto](https://github.com/gpetretto))

## [v1.0.0](https://github.com/Matgenix/jobflow-remote/tree/v1.0.0) (2026-01-14)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v0.1.8...v1.0.0)

**Closed issues:**

- Feature Request: Running Flows on Heterogeneous Resources [\#419](https://github.com/Matgenix/jobflow-remote/issues/419)
- How to run specific slurm script on server? [\#417](https://github.com/Matgenix/jobflow-remote/issues/417)
- Mixmatch between Runner log and submission file leading to error [\#397](https://github.com/Matgenix/jobflow-remote/issues/397)
- More than the `max_jobs` number of parallel batch jobs are submitted, after some time [\#391](https://github.com/Matgenix/jobflow-remote/issues/391)
- Runs fail the first time [\#379](https://github.com/Matgenix/jobflow-remote/issues/379)
- setting number of nodes and process in PBS jobs [\#378](https://github.com/Matgenix/jobflow-remote/issues/378)
- `jf runner stop` does not kill all `jf` processes when starting a new SSH session in a very specific way [\#373](https://github.com/Matgenix/jobflow-remote/issues/373)
- `jf project check --errors` does not check if the Slurm `resources` keys are valid but perhaps should [\#371](https://github.com/Matgenix/jobflow-remote/issues/371)
- Incorrect Environment Variable Name in Documentation \(JFREMOTE\_PROJECT\_FOLDER vs JFREMOTE\_PROJECTS\_FOLDER\) [\#362](https://github.com/Matgenix/jobflow-remote/issues/362)
- jobflow\_remote.config.base.ConfigError: ExecutionConfig with id example\_config is not defined [\#358](https://github.com/Matgenix/jobflow-remote/issues/358)
- Don't rely on there being an `additional_stores` field \(e.g. with Atomate2\) [\#331](https://github.com/Matgenix/jobflow-remote/issues/331)
- Unifying `jobstore` in Jobflow-Remote and `JOB_STORE` in Jobflow [\#328](https://github.com/Matgenix/jobflow-remote/issues/328)
- Python package checks during `jf project check --errors` are being done in the base environment [\#318](https://github.com/Matgenix/jobflow-remote/issues/318)
- Feature request: Pause flow to wait for user interaction [\#289](https://github.com/Matgenix/jobflow-remote/issues/289)
- Better error handling for incorrectly formatted project YAML [\#286](https://github.com/Matgenix/jobflow-remote/issues/286)
- name of "TERMINATED" state [\#101](https://github.com/Matgenix/jobflow-remote/issues/101)
- Suggestion: PSI/J for Interfacing with the Job Scheduler [\#10](https://github.com/Matgenix/jobflow-remote/issues/10)

**Merged pull requests:**

- remove contributing.rst [\#441](https://github.com/Matgenix/jobflow-remote/pull/441) ([gpetretto](https://github.com/gpetretto))
- Update release action [\#440](https://github.com/Matgenix/jobflow-remote/pull/440) ([gpetretto](https://github.com/gpetretto))
- qtoolkit 0.1.7 [\#437](https://github.com/Matgenix/jobflow-remote/pull/437) ([gpetretto](https://github.com/gpetretto))
- fix test and timezones [\#434](https://github.com/Matgenix/jobflow-remote/pull/434) ([gpetretto](https://github.com/gpetretto))
- Bump actions/download-artifact from 6 to 7 [\#428](https://github.com/Matgenix/jobflow-remote/pull/428) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/upload-artifact from 5 to 6 [\#427](https://github.com/Matgenix/jobflow-remote/pull/427) ([dependabot[bot]](https://github.com/apps/dependabot))
- Updates for batch jobs management [\#425](https://github.com/Matgenix/jobflow-remote/pull/425) ([gpetretto](https://github.com/gpetretto))
- Do not support python 3.14.1, as it breaks networkx [\#424](https://github.com/Matgenix/jobflow-remote/pull/424) ([gpetretto](https://github.com/gpetretto))
- Fix dev docs [\#422](https://github.com/Matgenix/jobflow-remote/pull/422) ([gpetretto](https://github.com/gpetretto))
- Runner updates [\#416](https://github.com/Matgenix/jobflow-remote/pull/416) ([gpetretto](https://github.com/gpetretto))
- Allow jf job set-state for many Jobs at once [\#413](https://github.com/Matgenix/jobflow-remote/pull/413) ([gpetretto](https://github.com/gpetretto))
- Fix job names in tests to prevent some random test failures [\#412](https://github.com/Matgenix/jobflow-remote/pull/412) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Bump actions/checkout from 5 to 6 [\#410](https://github.com/Matgenix/jobflow-remote/pull/410) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add warn option [\#407](https://github.com/Matgenix/jobflow-remote/pull/407) ([FabiPi3](https://github.com/FabiPi3))
- Remove support for python 3.9 [\#406](https://github.com/Matgenix/jobflow-remote/pull/406) ([gpetretto](https://github.com/gpetretto))
- fix upgrade to 1.0 [\#404](https://github.com/Matgenix/jobflow-remote/pull/404) ([gpetretto](https://github.com/gpetretto))
- Setting up uv to do lowest-direct and highest deps resolutions tests. [\#402](https://github.com/Matgenix/jobflow-remote/pull/402) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Replace --force with --yes [\#400](https://github.com/Matgenix/jobflow-remote/pull/400) ([gpetretto](https://github.com/gpetretto))
- Report option for jf flow info [\#399](https://github.com/Matgenix/jobflow-remote/pull/399) ([gpetretto](https://github.com/gpetretto))
- Update `set_run_config` to allow multiple changes [\#396](https://github.com/Matgenix/jobflow-remote/pull/396) ([gpetretto](https://github.com/gpetretto))
- Fix for batch submission [\#392](https://github.com/Matgenix/jobflow-remote/pull/392) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Add CLI option to edit project files [\#390](https://github.com/Matgenix/jobflow-remote/pull/390) ([gpetretto](https://github.com/gpetretto))
- CLI commands to remove job folders [\#388](https://github.com/Matgenix/jobflow-remote/pull/388) ([gpetretto](https://github.com/gpetretto))
- Bump actions/download-artifact from 5 to 6 [\#387](https://github.com/Matgenix/jobflow-remote/pull/387) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump actions/upload-artifact from 4 to 5 [\#386](https://github.com/Matgenix/jobflow-remote/pull/386) ([dependabot[bot]](https://github.com/apps/dependabot))
- Changed TERMINATED state to RUN\_FINISHED. [\#384](https://github.com/Matgenix/jobflow-remote/pull/384) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Added the possibility to have a custom scheduler io [\#383](https://github.com/Matgenix/jobflow-remote/pull/383) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Bugfix and small updates [\#376](https://github.com/Matgenix/jobflow-remote/pull/376) ([gpetretto](https://github.com/gpetretto))
- Clearer error message for undefined active project [\#374](https://github.com/Matgenix/jobflow-remote/pull/374) ([Andrew-S-Rosen](https://github.com/Andrew-S-Rosen))
- Improve warning message for flow reset validation [\#372](https://github.com/Matgenix/jobflow-remote/pull/372) ([Andrew-S-Rosen](https://github.com/Andrew-S-Rosen))
- Initial implementation of metadata in flow info. [\#366](https://github.com/Matgenix/jobflow-remote/pull/366) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Added check of environment variables in jf project check. [\#364](https://github.com/Matgenix/jobflow-remote/pull/364) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Removed auxiliary based management of batch jobs [\#363](https://github.com/Matgenix/jobflow-remote/pull/363) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Updates to batch process [\#354](https://github.com/Matgenix/jobflow-remote/pull/354) ([davidwaroquiers](https://github.com/davidwaroquiers))

## [v0.1.8](https://github.com/Matgenix/jobflow-remote/tree/v0.1.8) (2025-09-23)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v0.1.7...v0.1.8)

**Closed issues:**

- UnboundLocalError when running `jf job files get` [\#350](https://github.com/Matgenix/jobflow-remote/issues/350)
- Allow for the omission of a `jobstore` altogether [\#332](https://github.com/Matgenix/jobflow-remote/issues/332)
- Parallel batch job shows fewer `BATCH_RUNNING` jobs than expected after `jf admin reset` [\#326](https://github.com/Matgenix/jobflow-remote/issues/326)
- The built docs render `--` as `–` [\#322](https://github.com/Matgenix/jobflow-remote/issues/322)
- Feature Request: Flow\_DB\_ID option for jf job list command [\#320](https://github.com/Matgenix/jobflow-remote/issues/320)
- Unexpected keyword argument 'sort' [\#298](https://github.com/Matgenix/jobflow-remote/issues/298)
- Freshly created replaced jobs should be stopped as well [\#291](https://github.com/Matgenix/jobflow-remote/issues/291)
- `Using atomate2 with jobflow-remote` example failed to run: NBANDS seems to be too high. [\#280](https://github.com/Matgenix/jobflow-remote/issues/280)

**Merged pull requests:**

- Bump actions/download-artifact from 4 to 5 [\#349](https://github.com/Matgenix/jobflow-remote/pull/349) ([dependabot[bot]](https://github.com/apps/dependabot))
- Small fixes and tests improvements [\#347](https://github.com/Matgenix/jobflow-remote/pull/347) ([gpetretto](https://github.com/gpetretto))
- Fixture for run\_check\_cli that includes patching the consoles [\#346](https://github.com/Matgenix/jobflow-remote/pull/346) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Bump actions/setup-python from 5 to 6 [\#342](https://github.com/Matgenix/jobflow-remote/pull/342) ([dependabot[bot]](https://github.com/apps/dependabot))
- CLI plugins [\#339](https://github.com/Matgenix/jobflow-remote/pull/339) ([gpetretto](https://github.com/gpetretto))
- Bump actions/upload-pages-artifact from 3 to 4 [\#336](https://github.com/Matgenix/jobflow-remote/pull/336) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix batch worker [\#334](https://github.com/Matgenix/jobflow-remote/pull/334) ([gpetretto](https://github.com/gpetretto))
- Bump actions/checkout from 4 to 5 [\#330](https://github.com/Matgenix/jobflow-remote/pull/330) ([dependabot[bot]](https://github.com/apps/dependabot))
- Optional jobstores [\#327](https://github.com/Matgenix/jobflow-remote/pull/327) ([gpetretto](https://github.com/gpetretto))
- Dw/test cli colors [\#324](https://github.com/Matgenix/jobflow-remote/pull/324) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Merge custom and automatic queries [\#321](https://github.com/Matgenix/jobflow-remote/pull/321) ([FabiPi3](https://github.com/FabiPi3))
- Minor updates: color job list, allow resuming STOPPED jobs [\#316](https://github.com/Matgenix/jobflow-remote/pull/316) ([gpetretto](https://github.com/gpetretto))
- Include coverage from code executed remotely [\#255](https://github.com/Matgenix/jobflow-remote/pull/255) ([davidwaroquiers](https://github.com/davidwaroquiers))

## [v0.1.7](https://github.com/Matgenix/jobflow-remote/tree/v0.1.7) (2025-06-25)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v0.1.6...v0.1.7)

**Implemented enhancements:**

- SUGGESTION: `--count` option for `jf flow list` and `jf job list` [\#110](https://github.com/Matgenix/jobflow-remote/issues/110)

**Closed issues:**

- how to best cite jobflow-remote? [\#290](https://github.com/Matgenix/jobflow-remote/issues/290)
- Feature Request: New UX for setting job priority [\#287](https://github.com/Matgenix/jobflow-remote/issues/287)
- The long time interval between two jobs of a flow. [\#278](https://github.com/Matgenix/jobflow-remote/issues/278)
- The cores in the `submit.sh` is different from the one shown in the OUTCAR. [\#277](https://github.com/Matgenix/jobflow-remote/issues/277)
- Set a different `work_dir` from the one defined in the configuration file at runtime. [\#276](https://github.com/Matgenix/jobflow-remote/issues/276)
- Put CECAM slides in the docs? [\#273](https://github.com/Matgenix/jobflow-remote/issues/273)
- Where can we find information about the Slurm job metadata? [\#268](https://github.com/Matgenix/jobflow-remote/issues/268)
- How to dynamically change the worker configuration [\#267](https://github.com/Matgenix/jobflow-remote/issues/267)
- The `run_dir` text is often cutoff in the `jf job info ID` print-out [\#266](https://github.com/Matgenix/jobflow-remote/issues/266)
- Warn user to not use the same database for queue and output store [\#264](https://github.com/Matgenix/jobflow-remote/issues/264)
- How to solve the question: TypeError: 'dict' object is not callable [\#263](https://github.com/Matgenix/jobflow-remote/issues/263)
- a weird problem with job metadata [\#261](https://github.com/Matgenix/jobflow-remote/issues/261)
- How to update failed jobs input parameters ? [\#125](https://github.com/Matgenix/jobflow-remote/issues/125)

**Merged pull requests:**

- bugfix and test release workflow [\#313](https://github.com/Matgenix/jobflow-remote/pull/313) ([gpetretto](https://github.com/gpetretto))
- Update release.yml [\#310](https://github.com/Matgenix/jobflow-remote/pull/310) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Add --count option in CLI [\#307](https://github.com/Matgenix/jobflow-remote/pull/307) ([gpetretto](https://github.com/gpetretto))
- Fix testing, upgrade test requirements [\#302](https://github.com/Matgenix/jobflow-remote/pull/302) ([gpetretto](https://github.com/gpetretto))
- fix ignoring resolve\_references in JobConfig [\#288](https://github.com/Matgenix/jobflow-remote/pull/288) ([gpetretto](https://github.com/gpetretto))
- CLI and other small updates [\#269](https://github.com/Matgenix/jobflow-remote/pull/269) ([gpetretto](https://github.com/gpetretto))
- Reset stored data field on rerun [\#260](https://github.com/Matgenix/jobflow-remote/pull/260) ([FabiPi3](https://github.com/FabiPi3))
- Docker optimisations for CI [\#247](https://github.com/Matgenix/jobflow-remote/pull/247) ([ml-evs](https://github.com/ml-evs))

## [v0.1.6](https://github.com/Matgenix/jobflow-remote/tree/v0.1.6) (2025-02-07)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v0.1.5...v0.1.6)

**Closed issues:**

- Replacing failed jobs and adding new jobs to a flow [\#250](https://github.com/Matgenix/jobflow-remote/issues/250)

**Merged pull requests:**

- Bug fix for Remote Host [\#253](https://github.com/Matgenix/jobflow-remote/pull/253) ([gpetretto](https://github.com/gpetretto))
- Hot fix to pin pymongo version to lower than 4.11. [\#251](https://github.com/Matgenix/jobflow-remote/pull/251) ([davidwaroquiers](https://github.com/davidwaroquiers))

## [v0.1.5](https://github.com/Matgenix/jobflow-remote/tree/v0.1.5) (2025-01-29)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v0.1.4...v0.1.5)

**Implemented enhancements:**

- Optionally delay download after job end [\#210](https://github.com/Matgenix/jobflow-remote/issues/210)
- Add a backup feature [\#165](https://github.com/Matgenix/jobflow-remote/issues/165)

**Closed issues:**

- Print only with no wait [\#245](https://github.com/Matgenix/jobflow-remote/issues/245)
- Mismatched dumped taskdoc and database state [\#231](https://github.com/Matgenix/jobflow-remote/issues/231)
- Check status for simulation code in `jf job list` [\#227](https://github.com/Matgenix/jobflow-remote/issues/227)
- Successful Response not being written to jfremote\_out.json [\#226](https://github.com/Matgenix/jobflow-remote/issues/226)
- `jf admin upgrade` causes a lot of errors [\#224](https://github.com/Matgenix/jobflow-remote/issues/224)
- QToolKit is currently pinned to development version [\#218](https://github.com/Matgenix/jobflow-remote/issues/218)
- Full-split : Remote Runner setup [\#214](https://github.com/Matgenix/jobflow-remote/issues/214)
- An Error occurred during the command execution: ValueError No daemon runner document. [\#211](https://github.com/Matgenix/jobflow-remote/issues/211)
- "Error executing jf project check --errors to connect to \<cluster\> \(no additional info\)" [\#208](https://github.com/Matgenix/jobflow-remote/issues/208)
- how to rerun a job but with improved memory [\#198](https://github.com/Matgenix/jobflow-remote/issues/198)
- Delete VASP files when rerunnning jobs? [\#197](https://github.com/Matgenix/jobflow-remote/issues/197)
- call jf from container [\#196](https://github.com/Matgenix/jobflow-remote/issues/196)
- how to limit number of jobs [\#195](https://github.com/Matgenix/jobflow-remote/issues/195)
- non-unique uuids as a problem in jobflow-remote [\#193](https://github.com/Matgenix/jobflow-remote/issues/193)
- Set `projects_folder` via env var [\#188](https://github.com/Matgenix/jobflow-remote/issues/188)
- Support for heterogeneous computing resources? [\#184](https://github.com/Matgenix/jobflow-remote/issues/184)
- Jobflow remote logo [\#178](https://github.com/Matgenix/jobflow-remote/issues/178)
- How could I use SGE for job submission?  [\#159](https://github.com/Matgenix/jobflow-remote/issues/159)
- Check there is not already a runner running [\#140](https://github.com/Matgenix/jobflow-remote/issues/140)
- How to add metadata to `flows` docs? [\#124](https://github.com/Matgenix/jobflow-remote/issues/124)
- Job submission fails based on job name [\#112](https://github.com/Matgenix/jobflow-remote/issues/112)
- preventing reset issues [\#102](https://github.com/Matgenix/jobflow-remote/issues/102)
- Missing docs: batch mode [\#96](https://github.com/Matgenix/jobflow-remote/issues/96)
- Does jobflow-remote support the pilot job model? [\#86](https://github.com/Matgenix/jobflow-remote/issues/86)

**Merged pull requests:**

- bump qtoolkit 0.1.6 [\#248](https://github.com/Matgenix/jobflow-remote/pull/248) ([gpetretto](https://github.com/gpetretto))
- Make JobDoc available at runtime [\#244](https://github.com/Matgenix/jobflow-remote/pull/244) ([gpetretto](https://github.com/gpetretto))
- Finalize gui implementation [\#243](https://github.com/Matgenix/jobflow-remote/pull/243) ([gpetretto](https://github.com/gpetretto))
- fix typing for submit\_flow [\#242](https://github.com/Matgenix/jobflow-remote/pull/242) ([FabiPi3](https://github.com/FabiPi3))
- Web GUI for jobflow remote [\#241](https://github.com/Matgenix/jobflow-remote/pull/241) ([fraricci](https://github.com/fraricci))
- Allow switching off daemon if no running runner doc [\#240](https://github.com/Matgenix/jobflow-remote/pull/240) ([gpetretto](https://github.com/gpetretto))
- Logo [\#239](https://github.com/Matgenix/jobflow-remote/pull/239) ([gpetretto](https://github.com/gpetretto))
- \[WIP\] Save db dumps for test failures [\#237](https://github.com/Matgenix/jobflow-remote/pull/237) ([gpetretto](https://github.com/gpetretto))
- Bump docker/bake-action from 5 to 6 [\#236](https://github.com/Matgenix/jobflow-remote/pull/236) ([dependabot[bot]](https://github.com/apps/dependabot))
- Various updates [\#234](https://github.com/Matgenix/jobflow-remote/pull/234) ([gpetretto](https://github.com/gpetretto))
- \[WIP\] test direct execution of docker commands and addition of PBS container [\#233](https://github.com/Matgenix/jobflow-remote/pull/233) ([gpetretto](https://github.com/gpetretto))
- Testing adding flags for codecov, separating unit and integration tests. [\#232](https://github.com/Matgenix/jobflow-remote/pull/232) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Option for stored data in `jf job list` [\#228](https://github.com/Matgenix/jobflow-remote/pull/228) ([FabiPi3](https://github.com/FabiPi3))
- Bump codecov/codecov-action from 4 to 5 [\#209](https://github.com/Matgenix/jobflow-remote/pull/209) ([dependabot[bot]](https://github.com/apps/dependabot))
- Delete job files when rerunning; customizable execution command [\#201](https://github.com/Matgenix/jobflow-remote/pull/201) ([gpetretto](https://github.com/gpetretto))
- Add sanitization option to host outputs [\#191](https://github.com/Matgenix/jobflow-remote/pull/191) ([gpetretto](https://github.com/gpetretto))
- Add backup functionality [\#190](https://github.com/Matgenix/jobflow-remote/pull/190) ([gpetretto](https://github.com/gpetretto))
- Require date to reset DB [\#189](https://github.com/Matgenix/jobflow-remote/pull/189) ([gpetretto](https://github.com/gpetretto))
- Allow list of jobs from scheduler using username [\#187](https://github.com/Matgenix/jobflow-remote/pull/187) ([gpetretto](https://github.com/gpetretto))
- Option to set Job priority [\#183](https://github.com/Matgenix/jobflow-remote/pull/183) ([gpetretto](https://github.com/gpetretto))
- New CLI functionalities: tree, report, job info [\#180](https://github.com/Matgenix/jobflow-remote/pull/180) ([gpetretto](https://github.com/gpetretto))
- Parallel batch submission [\#172](https://github.com/Matgenix/jobflow-remote/pull/172) ([gpetretto](https://github.com/gpetretto))
- Generalize integration tests to other queue systems \(SGE\) and test more Python versions [\#160](https://github.com/Matgenix/jobflow-remote/pull/160) ([ml-evs](https://github.com/ml-evs))
- Project check [\#158](https://github.com/Matgenix/jobflow-remote/pull/158) ([davidwaroquiers](https://github.com/davidwaroquiers))
- WIP: Check only one runner [\#150](https://github.com/Matgenix/jobflow-remote/pull/150) ([davidwaroquiers](https://github.com/davidwaroquiers))

## [v0.1.4](https://github.com/Matgenix/jobflow-remote/tree/v0.1.4) (2024-09-13)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v0.1.3...v0.1.4)

**Closed issues:**

- Remote errors in interactive mode when larger workflows are run [\#174](https://github.com/Matgenix/jobflow-remote/issues/174)
- Add  `--query` \(or `--worker`\) flag to `jf job rerun/retry` [\#170](https://github.com/Matgenix/jobflow-remote/issues/170)

**Merged pull requests:**

- Bugfix: improve collection indexes to prevent duplications [\#177](https://github.com/Matgenix/jobflow-remote/pull/177) ([gpetretto](https://github.com/gpetretto))
- Add --worker and --query option in CLI [\#171](https://github.com/Matgenix/jobflow-remote/pull/171) ([gpetretto](https://github.com/gpetretto))

## [v0.1.3](https://github.com/Matgenix/jobflow-remote/tree/v0.1.3) (2024-09-02)

[Full Changelog](https://github.com/Matgenix/jobflow-remote/compare/v0.1.2...v0.1.3)

**Closed issues:**

- Cannot force delete more than 10 flows [\#169](https://github.com/Matgenix/jobflow-remote/issues/169)
- Issue with some db in DOWNLOAD state [\#161](https://github.com/Matgenix/jobflow-remote/issues/161)
- Jobs fail with error `Remote error: file path/to/job_dir/remote_job_data.json for job xxx does not exist` [\#157](https://github.com/Matgenix/jobflow-remote/issues/157)
- Correct setup of jfremote config project.yaml file for nested gateways [\#153](https://github.com/Matgenix/jobflow-remote/issues/153)
- Is there a way to visualize the workflow based on jobflow-remote info? [\#149](https://github.com/Matgenix/jobflow-remote/issues/149)
- Flow information is shared even when using different queue stores [\#144](https://github.com/Matgenix/jobflow-remote/issues/144)
- a suggestion to improve the documentation [\#143](https://github.com/Matgenix/jobflow-remote/issues/143)
- Delete job functionality [\#141](https://github.com/Matgenix/jobflow-remote/issues/141)
- Queue-out interactive mode [\#90](https://github.com/Matgenix/jobflow-remote/issues/90)
- Writing to home dir is crashing my submission. [\#81](https://github.com/Matgenix/jobflow-remote/issues/81)

**Merged pull requests:**

- Minor updates [\#156](https://github.com/Matgenix/jobflow-remote/pull/156) ([gpetretto](https://github.com/gpetretto))
- delete\_job functionality [\#154](https://github.com/Matgenix/jobflow-remote/pull/154) ([gpetretto](https://github.com/gpetretto))
- Fix ruamel yaml dump [\#151](https://github.com/Matgenix/jobflow-remote/pull/151) ([davidwaroquiers](https://github.com/davidwaroquiers))
- Add hyperlink for slurm and pbs resource keywords as a note in the documentation [\#148](https://github.com/Matgenix/jobflow-remote/pull/148) ([QuantumChemist](https://github.com/QuantumChemist))
- CLI add `--pid` option to `jf job info` [\#142](https://github.com/Matgenix/jobflow-remote/pull/142) ([janosh](https://github.com/janosh))
- Fix legacy `ruff` errors and enable corresponding rules for future linting [\#134](https://github.com/Matgenix/jobflow-remote/pull/134) ([janosh](https://github.com/janosh))

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
- Feature request: support psutil 6.0.0 [\#128](https://github.com/Matgenix/jobflow-remote/issues/128)
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
