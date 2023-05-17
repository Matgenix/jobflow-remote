"""jobflow-remote is a python package to run jobflow workflows on remote resources"""

from jobflow_remote._version import __version__
from jobflow_remote.config.settings import JobflowRemoteSettings

SETTINGS = JobflowRemoteSettings()
