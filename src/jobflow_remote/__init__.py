"""jobflow-remote is a python package to run jobflow workflows on remote resources"""

from jobflow_remote._version import __version__
from jobflow_remote.config.jobconfig import set_run_config
from jobflow_remote.config.manager import ConfigManager
from jobflow_remote.config.settings import JobflowRemoteSettings
from jobflow_remote.jobs.jobcontroller import JobController
from jobflow_remote.jobs.store import get_jobstore
from jobflow_remote.jobs.submit import submit_flow

SETTINGS = JobflowRemoteSettings()
