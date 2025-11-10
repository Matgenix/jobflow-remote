"""This script was used to create the backup of the database collections used before a release that needs
an upgrade. It can be used as a template for future upgrade tests that require a dump of the database collections."""

import os
import sys
import tempfile

import pymongo
from monty.serialization import dumpfn

import jobflow_remote
from jobflow_remote import SETTINGS, JobController, submit_flow
from jobflow_remote.config.base import Project
from jobflow_remote.jobs.state import JobState
from jobflow_remote.testing import add_sleep

# Check that we use the right version
version_to_backup = "0.1.8"
if jobflow_remote.__version__ != version_to_backup:
    sys.exit(
        f"Backup should be made using version {version_to_backup}, current version is {jobflow_remote.__version__}"
    )


# Make sure we have a database and it's empty
DB_HOST = "localhost"
DB_PORT = 27017
DB_NAME = "jobflow_remote_backup_creation_tests__"
mc = pymongo.MongoClient(host=DB_HOST, port=DB_PORT)
# try connecting to the DB with a short delay, since the DB is local it
# should not take long to reply
try:
    with pymongo.timeout(1):
        mc.server_info()
except Exception as e:
    sys.exit(f"Could not connect to a local DB {getattr(e, 'message', str(e))}")
database = mc[DB_NAME]
collections = list(database.list_collections())
if collections:
    print(
        f"Found {len(collections)} collections in the database used to create backups for the tests:"
    )
    for collection in collections:
        print(f' - {collection["name"]}')
    confirm = input('Drop the database ? (type "y" to confirm) ... ')
    if confirm == "y":
        mc.drop_database(DB_NAME)
    else:
        sys.exit("Cannot continue without an empty database")

# Initialize the project and job controller
BASE_DIR = tempfile.mkdtemp()
PROJECTS_DIR = os.path.join(BASE_DIR, "projects")
os.makedirs(PROJECTS_DIR)
store_kv = {"type": "MongoStore", "database": DB_NAME, "host": DB_HOST, "port": DB_PORT}
project = Project(
    name="random_project_name",
    base_dir=BASE_DIR,
    jobstore={
        "docs_store": {
            **store_kv,
            "collection_name": "docs",
        },
    },
    queue={
        "store": {
            **store_kv,
            "collection_name": "jobs",
        },
    },
    workers={
        "test_local_worker": dict(
            type="local",
            scheduler_type="shell",
            work_dir=os.path.join(BASE_DIR, "local_worker"),
            resources={},
        ),
        "test_local_batch_worker": dict(
            type="local",
            scheduler_type="shell",
            work_dir=os.path.join(BASE_DIR, "local_batch_worker"),
            batch={
                "jobs_handle_dir": os.path.join(BASE_DIR, "local_batch_worker_handle"),
                "work_dir": os.path.join(BASE_DIR, "local_batch_worker_work"),
                "max_wait": 3,
            },
            max_jobs=4,
        ),
    },
)
dumpfn(project, os.path.join(PROJECTS_DIR, f"{project.name}.json"))
SETTINGS.projects_folder = PROJECTS_DIR
job_controller = JobController.from_project_name(project_name=project.name)

# Initialize the collections
job_controller.reset()

j = add_sleep(1, 0.5)
db_ids = submit_flow(j, worker="test_local_worker", project=project.name)

job_controller.set_job_state(state=JobState.TERMINATED, db_id=db_ids[0])

db_dump_dir = "dump"
coll_dump_dir = os.path.join(db_dump_dir, DB_NAME)
dump_info = job_controller.backup_dump(db_dump_dir, compress=True, python=True)

# Move files to this directory
for fname in os.listdir(coll_dump_dir):
    if os.path.exists(fname):
        print("It exists! Overwrite ...")
    os.replace(os.path.join(coll_dump_dir, fname), fname)
os.removedirs(coll_dump_dir)
