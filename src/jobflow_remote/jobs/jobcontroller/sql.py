from __future__ import annotations

import contextlib
import json
import logging
import warnings
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import jobflow
import pymongo
from jobflow import JobStore, OnMissing
from monty.json import MontyDecoder, MontyEncoder
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
    and_,
    create_engine,
    delete,
    func,
    or_,
    select,
    update,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

from jobflow_remote.jobs.data import (
    DynamicResponseType,
    FlowDoc,
    FlowInfo,
    JobDoc,
    JobInfo,
    get_initial_flow_doc_dict,
    get_initial_job_doc_dict,
    get_reset_job_base_dict,
)
from jobflow_remote.jobs.jobcontroller.base import JobController
from jobflow_remote.jobs.state import (
    DELETABLE_STATES,
    PAUSABLE_STATES,
    RESETTABLE_STATES,
    RUNNING_STATES,
    FlowState,
    JobState,
)
from jobflow_remote.utils.data import suuid
from jobflow_remote.utils.db import (
    FlowLockedError,
    JobLockedError,
    LockedDocumentError,
    MissingDocumentError,
)
from jobflow_remote.utils.remote import SharedHosts, safe_remove_job_files

if TYPE_CHECKING:
    from collections.abc import Sequence
    from collections.abc import Generator
    from jobflow_remote.config.base import ExecutionConfig, Project


logger = logging.getLogger(__name__)


class MontyJSON(TypeDecorator):
    """
    JSON column type that uses MontyEncoder/MontyDecoder for datetime serialization.

    This provides the benefits of native JSON storage (SQL JSON operators, indexing)
    while properly handling datetime objects through Monty serialization.
    """

    impl = JSON  # Underlying column is native JSON
    cache_ok = True

    def process_bind_param(self, value, dialect):
        """Process data going TO the database."""
        if value is not None:
            # Convert datetime objects to ISO strings using MontyEncoder
            # The JSON column will handle the actual JSON serialization
            return json.loads(json.dumps(value, cls=MontyEncoder))
        return value

    def process_result_value(self, value, dialect):
        """Process data coming FROM the database."""
        if value is not None:
            # Use MontyDecoder to restore datetime objects from ISO strings
            return json.loads(json.dumps(value), cls=MontyDecoder)
        return value


class MongoToSQLConverter:
    """
    Converts MongoDB-style queries to SQLAlchemy expressions.

    Handles common MongoDB operators like $exists, $in, $not, $gt, etc.
    """

    @staticmethod
    def convert_query(query: dict, table_class) -> list:
        """
        Convert a MongoDB-style query dictionary to SQLAlchemy conditions.

        Parameters
        ----------
        query
            MongoDB-style query dictionary
        table_class
            SQLAlchemy table class to query against

        Returns
        -------
        list
            List of SQLAlchemy conditions
        """
        conditions = []

        # logger.info(f"MongoToSQL: Converting query {query} for table {table_class.__tablename__}")

        for key, value in query.items():
            if key.startswith("$"):
                # Handle top-level logical operators
                if key == "$and":
                    and_conditions = []
                    for sub_query in value:
                        sub_conditions = MongoToSQLConverter.convert_query(
                            sub_query, table_class
                        )
                        if len(sub_conditions) == 1:
                            and_conditions.append(sub_conditions[0])
                        elif len(sub_conditions) > 1:
                            and_conditions.append(and_(*sub_conditions))
                    if and_conditions:
                        conditions.append(and_(*and_conditions))
                elif key == "$or":
                    or_conditions = []
                    for sub_query in value:
                        sub_conditions = MongoToSQLConverter.convert_query(
                            sub_query, table_class
                        )
                        if len(sub_conditions) == 1:
                            or_conditions.append(sub_conditions[0])
                        elif len(sub_conditions) > 1:
                            # Multiple conditions in a sub-query should be ANDed together
                            or_conditions.append(and_(*sub_conditions))
                    if or_conditions:
                        conditions.append(or_(*or_conditions))
            else:
                # Handle field-specific queries
                condition = MongoToSQLConverter._convert_field_query(
                    key, value, table_class
                )
                # logger.info(f"MongoToSQL: Field '{key}' = '{value}' → condition: {condition}")
                if condition is not None:
                    conditions.append(condition)

        # logger.info(f"MongoToSQL: Final conditions ({len(conditions)}): {conditions}")
        return conditions

    @staticmethod
    def _convert_field_query(field: str, value, table_class):
        """Convert a single field query to SQLAlchemy condition."""
        from sqlalchemy import func

        # Handle special field-specific queries for flows
        if field == "jobs":
            # Handle queries like {"jobs": job_id} for flows containing a specific job
            if isinstance(value, str):
                # Simple job ID search in the jobs JSON array
                return table_class.jobs.contains(f'"{value}"')
            return None
        elif field == "ids":
            # Handle queries like {"ids": {"$elemMatch": {"0": db_id}}}
            if isinstance(value, dict) and "$elemMatch" in value:
                elem_match = value["$elemMatch"]
                if "0" in elem_match:
                    # Search for db_id in the ids JSON array
                    db_id = elem_match["0"]
                    return table_class.ids.contains(f'"{db_id}"')
            return None

        # Handle nested field paths (e.g., "remote.retry_time_limit")
        if "." in field:
            # For nested fields, we need to handle them based on how they're stored
            if field == "remote.retry_time_limit":
                # This maps to the flattened remote_retry_time_limit column
                field = "remote_retry_time_limit"
            elif field == "remote.step_attempts":
                field = "remote_step_attempts"
            elif field == "remote.queue_state":
                field = "remote_queue_state"
            elif field.startswith("job."):
                # Handle job JSON field queries using proper JSON operators
                json_path = field[4:]  # Remove "job." prefix
                if json_path == "name":
                    # Use SQLite's ->> operator for top-level key
                    name_field = table_class.job.op("->>")("name")

                    # Special handling for job name regex
                    if isinstance(value, dict) and "$regex" in value:
                        regex_pattern = value["$regex"]
                        # Convert fnmatch regex pattern to SQL LIKE pattern

                        # Handle patterns created by fnmatch.translate()
                        if regex_pattern.startswith("^") and regex_pattern.endswith(
                            "\\Z"
                        ):
                            # Strip anchors and regex syntax: ^(?s:add1)\Z -> add1
                            inner_pattern = regex_pattern[1:-2]  # Remove ^ and \Z
                            if inner_pattern.startswith(
                                "(?s:"
                            ) and inner_pattern.endswith(")"):
                                # Remove (?s:...) wrapper -> add1
                                clean_pattern = inner_pattern[4:-1]
                                # Convert basic regex to LIKE: .* -> %, . -> _
                                like_pattern = clean_pattern.replace(".*", "%").replace(
                                    ".", "_"
                                )
                                return name_field.like(like_pattern)

                        # Fallback: basic regex to LIKE conversion
                        if regex_pattern.startswith("^"):
                            like_pattern = (
                                regex_pattern[1:].replace(".*", "%").replace(".", "_")
                            )
                            return name_field.like(like_pattern)
                        else:
                            like_pattern = regex_pattern.replace(".*", "%").replace(
                                ".", "_"
                            )
                            return name_field.like(f"%{like_pattern}%")
                    else:
                        # Direct name comparison
                        return name_field == value
                elif json_path.startswith("metadata."):
                    # Handle metadata queries like job.metadata.key
                    metadata_key = json_path[9:]  # Remove "metadata." prefix
                    # Use SQLite JSON operators for metadata access
                    # For string values, use ->> for unquoted strings; for others use -> for typed values
                    if isinstance(value, str):
                        extracted_field = table_class.job.op("->>")(
                            "$.metadata." + metadata_key
                        )
                    else:
                        extracted_field = func.json_extract(
                            table_class.job, f"$.metadata.{metadata_key}"
                        )
                    return extracted_field == value
                elif json_path == "hosts":
                    # Handle job.hosts queries for flow membership
                    if isinstance(value, dict) and "$in" in value:
                        host_uuids = value["$in"]
                        conditions = []
                        for host_uuid in host_uuids:
                            # Check if host_uuid is in the hosts array
                            conditions.append(
                                table_class.job["hosts"].contains([host_uuid])
                            )
                        return or_(*conditions) if conditions else None
                    else:
                        return table_class.job["hosts"].contains([value])
                else:
                    # Generic JSON field access - use SQLite JSON operators
                    # For string values, use ->> for unquoted strings; for others use -> for typed values
                    if isinstance(value, str):
                        # Use $.path for nested paths, or just key for top-level
                        path = f"$.{json_path}" if "." in json_path else json_path
                        extracted_field = table_class.job.op("->>")(path)
                    else:
                        extracted_field = func.json_extract(
                            table_class.job, f"$.{json_path}"
                        )
                    return extracted_field == value
                return None
            # Add more mappings as needed

        # For auxiliary table, handle document-based queries on JSON content
        if table_class.__tablename__ == "auxiliary":
            # Handle queries like {"running_runner": {"$exists": True}}
            # This should query the JSON content, not the doc_id
            if isinstance(value, dict) and "$exists" in value:
                if value["$exists"]:
                    # Field exists in JSON document - use JSON_TYPE to check field existence
                    # This handles both null and non-null values properly
                    # JSON_TYPE returns NULL only if the path doesn't exist
                    return func.json_type(table_class.document, f"$.{field}").isnot(
                        None
                    )
                else:
                    # Field doesn't exist in JSON document
                    return func.json_type(table_class.document, f"$.{field}").is_(None)
            else:
                # For other auxiliary queries, match the field value in JSON content
                # e.g., {"running_runner": some_value} should match JSON content
                return func.json_extract(table_class.document, f"$.{field}") == value

        # Check if field exists as column in the table
        if not hasattr(table_class, field):
            logger.warning(
                f"Field '{field}' not found in table {table_class.__tablename__}"
            )
            return None

        column = getattr(table_class, field)
        return MongoToSQLConverter._convert_value_query(column, value)

    @staticmethod
    def _convert_value_query(column, value):
        """Convert value part of query to SQLAlchemy condition."""
        if isinstance(value, dict):
            # Handle MongoDB operators
            conditions = []
            for op, op_value in value.items():
                if op == "$in":
                    if isinstance(op_value, (list, tuple)):
                        # Handle JobState and other enum conversions
                        converted_values = []
                        for v in op_value:
                            if hasattr(v, "value"):  # Enum-like objects
                                converted_values.append(v.value)
                            else:
                                converted_values.append(v)
                        # logger.debug(f"$in operator: original={op_value}, converted={converted_values}")

                        if converted_values:  # Only add condition if list is not empty
                            conditions.append(column.in_(converted_values))
                        else:
                            # logger.warning(f"Empty list for $in operator on column {column}")
                            # Empty list means no matches - add impossible condition
                            conditions.append(
                                column.is_(None) & column.is_not(None)
                            )  # Always False
                elif op == "$not":
                    # Handle nested $not conditions
                    if isinstance(op_value, dict):
                        for nested_op, nested_value in op_value.items():
                            if nested_op == "$gt":
                                # Special case: $not: {$gt: value} means <= value OR IS NULL
                                conditions.append(
                                    or_(column.is_(None), column <= nested_value)
                                )
                            elif nested_op == "$in":
                                converted_values = []
                                for v in nested_value:
                                    if hasattr(v, "value"):
                                        converted_values.append(v.value)
                                    else:
                                        converted_values.append(v)
                                conditions.append(~column.in_(converted_values))
                            # Add more $not cases as needed
                    else:
                        conditions.append(column != op_value)
                elif op == "$exists":
                    if op_value:
                        conditions.append(column.is_not(None))
                    else:
                        conditions.append(column.is_(None))
                elif op == "$gt":
                    conditions.append(column > op_value)
                elif op == "$gte":
                    conditions.append(column >= op_value)
                elif op == "$lt":
                    conditions.append(column < op_value)
                elif op == "$lte":
                    conditions.append(column <= op_value)
                elif op == "$ne":
                    conditions.append(column != op_value)
                elif op == "$regex":
                    # Convert MongoDB regex to SQL LIKE
                    conditions.append(column.like(f"%{op_value}%"))
                else:
                    logger.warning(f"Unsupported MongoDB operator: {op}")

            if len(conditions) == 1:
                return conditions[0]
            elif len(conditions) > 1:
                return and_(*conditions)
        elif hasattr(value, "value"):  # Handle enum-like objects
            return column == value.value
        else:
            return column == value

        return None

    @staticmethod
    def convert_field_names_for_update(updates: dict, table_class) -> dict:
        """
        Convert MongoDB field names to SQL column names for UPDATE operations.

        Parameters
        ----------
        updates
            Dictionary with MongoDB-style field names
        table_class
            SQLAlchemy table class

        Returns
        -------
        dict
            Dictionary with SQL column names
        """
        converted = {}

        for field, value in updates.items():
            # Handle nested field mappings
            if field.startswith("remote."):
                # Map remote.field_name to remote_field_name
                sql_field = field.replace(".", "_")
            else:
                sql_field = field

            # Only include fields that exist as columns in the table
            if hasattr(table_class, sql_field):
                converted[sql_field] = value
            else:
                logger.warning(
                    f"Field '{sql_field}' not found in table {table_class.__tablename__}, skipping update"
                )

        return converted


Base = declarative_base()


class SQLJob(Base):
    """SQL table for job documents."""

    __tablename__ = "jobs"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Job identification
    uuid = Column(String(36), nullable=False, index=True)
    index = Column(Integer, nullable=False, index=True)
    db_id = Column(String(50), nullable=False, unique=True, index=True)

    # Unique constraint on (uuid, index) combination like MongoDB
    __table_args__ = (UniqueConstraint("uuid", "index", name="uq_job_uuid_index"),)

    # Job content (JSON field)
    job = Column(JSON, nullable=False)  # JSON column for proper querying

    # Job metadata
    worker = Column(String(100), nullable=False, index=True)
    state = Column(String(20), nullable=False, index=True)
    parents = Column(Text)  # JSON array of parent UUIDs
    previous_state = Column(String(20))
    error = Column(Text)

    # Locking
    lock_id = Column(String(36))
    lock_time = Column(DateTime)

    # Execution info
    run_dir = Column(String(500))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    priority = Column(Integer, default=0, index=True)

    # Timestamps
    created_on = Column(DateTime, default=datetime.utcnow, index=True)
    updated_on = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, index=True
    )

    # Configuration
    exec_config = Column(Text)  # JSON
    resources = Column(Text)  # JSON

    # Remote execution info
    remote_step_attempts = Column(Integer, default=0)
    remote_queue_state = Column(String(20))
    remote_process_id = Column(String(100))
    remote_retry_time_limit = Column(DateTime)
    remote_error = Column(Text)
    remote_prerun_cleanup = Column(Boolean, default=False)
    remote_queue_out = Column(Text)
    remote_queue_err = Column(Text)

    # Stored data
    stored_data = Column(Text)  # JSON

    def to_job_doc(self) -> JobDoc:
        """Convert SQL row to JobDoc pydantic model."""
        job_data = self.job  # JSON column already provides dict

        # Reconstruct remote info
        from jobflow_remote.jobs.data import RemoteInfo

        remote = RemoteInfo(
            step_attempts=self.remote_step_attempts or 0,
            queue_state=self.remote_queue_state,
            process_id=self.remote_process_id,
            retry_time_limit=self.remote_retry_time_limit,
            error=self.remote_error,
            prerun_cleanup=self.remote_prerun_cleanup or False,
            queue_out=self.remote_queue_out,
            queue_err=self.remote_queue_err,
        )

        # Parse optional JSON fields
        parents = json.loads(self.parents) if self.parents else None

        # Handle exec_config - can be stored as JSON string (dict) or plain string
        exec_config = None
        if self.exec_config:
            try:
                exec_config = json.loads(self.exec_config)
            except json.JSONDecodeError:
                # If JSON parsing fails, it's a plain string
                exec_config = self.exec_config

        # Handle resources - can be stored as JSON string (dict) or plain string
        resources = None
        if self.resources:
            try:
                resources = json.loads(self.resources)
            except json.JSONDecodeError:
                # If JSON parsing fails, it's a plain string
                resources = self.resources

        stored_data = json.loads(self.stored_data) if self.stored_data else None

        return JobDoc(
            job=jobflow.Job.from_dict(job_data),
            uuid=self.uuid,
            index=self.index,
            db_id=self.db_id,
            worker=self.worker,
            state=JobState(self.state),
            remote=remote,
            parents=parents,
            previous_state=JobState(self.previous_state)
            if self.previous_state
            else None,
            error=self.error,
            lock_id=self.lock_id,
            lock_time=self.lock_time,
            run_dir=self.run_dir,
            start_time=self.start_time,
            end_time=self.end_time,
            created_on=self.created_on,
            updated_on=self.updated_on,
            priority=self.priority,
            exec_config=exec_config,
            resources=resources,
            stored_data=stored_data,
        )

    @classmethod
    def from_job_doc(cls, job_doc: JobDoc) -> SQLJob:
        """Create SQL row from JobDoc pydantic model."""
        return cls(
            uuid=job_doc.uuid,
            index=job_doc.index,
            db_id=job_doc.db_id,
            job=job_doc.as_db_dict()["job"],  # Use properly serialized job data
            worker=job_doc.worker,
            state=job_doc.state.value,
            parents=json.dumps(job_doc.parents) if job_doc.parents else None,
            previous_state=job_doc.previous_state.value
            if job_doc.previous_state
            else None,
            error=job_doc.error,
            lock_id=job_doc.lock_id,
            lock_time=job_doc.lock_time,
            run_dir=job_doc.run_dir,
            start_time=job_doc.start_time,
            end_time=job_doc.end_time,
            created_on=job_doc.created_on,
            updated_on=job_doc.updated_on,
            priority=job_doc.priority,
            exec_config=json.dumps(job_doc.exec_config)
            if job_doc.exec_config
            else None,
            resources=json.dumps(job_doc.resources) if job_doc.resources else None,
            remote_step_attempts=job_doc.remote.step_attempts,
            remote_queue_state=job_doc.remote.queue_state.value
            if job_doc.remote.queue_state
            else None,
            remote_process_id=job_doc.remote.process_id,
            remote_retry_time_limit=job_doc.remote.retry_time_limit,
            remote_error=job_doc.remote.error,
            remote_prerun_cleanup=job_doc.remote.prerun_cleanup,
            remote_queue_out=job_doc.remote.queue_out,
            remote_queue_err=job_doc.remote.queue_err,
            stored_data=json.dumps(job_doc.stored_data)
            if job_doc.stored_data
            else None,
        )


class SQLFlow(Base):
    """SQL table for flow documents."""

    __tablename__ = "flows"

    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Flow identification
    uuid = Column(String(36), nullable=False, unique=True, index=True)

    # Flow metadata
    name = Column(String(200), nullable=False, index=True)
    state = Column(String(20), nullable=False, index=True)

    # Locking
    lock_id = Column(String(36))
    lock_time = Column(DateTime)

    # Timestamps
    created_on = Column(DateTime, default=datetime.utcnow, index=True)
    updated_on = Column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, index=True
    )

    # Flow data (JSON fields)
    jobs = Column(JSON, nullable=False)  # JSON array of job UUIDs
    flow_metadata = Column(JSON)  # JSON metadata dict
    parents = Column(Text)  # JSON nested dict of parents
    ids = Column(Text)  # JSON array of (db_id, uuid, index) tuples
    jobstore = Column(String(100))

    def to_flow_doc(self) -> FlowDoc:
        """Convert SQL row to FlowDoc pydantic model."""
        # Load and sanitize parents - ensure all values are lists, not None
        parents_dict = {}
        if self.parents:
            raw_parents = json.loads(self.parents)
            for job_uuid, indices_dict in raw_parents.items():
                if isinstance(indices_dict, dict):
                    parents_dict[job_uuid] = {}
                    for idx, parent_list in indices_dict.items():
                        # Ensure parent_list is always a list, convert None to empty list
                        parents_dict[job_uuid][idx] = (
                            parent_list if parent_list is not None else []
                        )
                else:
                    # Handle legacy format if needed
                    parents_dict[job_uuid] = (
                        indices_dict if indices_dict is not None else {}
                    )

        return FlowDoc(
            uuid=self.uuid,
            jobs=self.jobs,
            state=FlowState(self.state),
            name=self.name,
            lock_id=self.lock_id,
            lock_time=self.lock_time,
            created_on=self.created_on,
            updated_on=self.updated_on,
            metadata=self.flow_metadata if self.flow_metadata else {},
            parents=parents_dict,
            ids=json.loads(self.ids) if self.ids else [],
            jobstore=self.jobstore,
        )

    @classmethod
    def from_flow_doc(cls, flow_doc: FlowDoc) -> SQLFlow:
        """Create SQL row from FlowDoc pydantic model."""
        # Ensure job UUIDs are unique
        unique_jobs = list(
            dict.fromkeys(flow_doc.jobs)
        )  # Preserves order while removing duplicates

        return cls(
            uuid=flow_doc.uuid,
            jobs=unique_jobs,  # Store unique job UUIDs as list for JSON column
            state=flow_doc.state.value,
            name=flow_doc.name,
            lock_id=flow_doc.lock_id,
            lock_time=flow_doc.lock_time,
            created_on=flow_doc.created_on,
            updated_on=flow_doc.updated_on,
            flow_metadata=flow_doc.metadata,
            parents=json.dumps(flow_doc.parents),
            ids=json.dumps(flow_doc.ids),
            jobstore=flow_doc.jobstore,
        )


class SQLAuxiliary(Base):
    """SQL table for auxiliary data - stores documents as JSON like MongoDB."""

    __tablename__ = "auxiliary"

    # Primary key (auto-increment for SQL)
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Document ID (equivalent to MongoDB's _id)
    doc_id = Column(String(100), nullable=False, unique=True, index=True)

    # Full document stored as JSON with Monty serialization for datetime support
    document = Column(
        MontyJSON, nullable=False
    )  # Native JSON document with Monty support

    # Locking for specific auxiliary documents
    lock_id = Column(String(36))
    lock_time = Column(DateTime)

    created_on = Column(DateTime, default=datetime.utcnow)
    updated_on = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self) -> dict:
        """Convert SQL row to MongoDB-like document dictionary."""
        # With JSON column type and custom deserializer, document is already deserialized
        doc = self.document if self.document else {}

        # Ensure doc is a dictionary
        if not isinstance(doc, dict):
            doc = {}

        # Create a copy to avoid modifying the original
        doc = dict(doc)

        # Add MongoDB-like metadata that aren't part of the stored document
        doc["_id"] = self.doc_id
        doc["lock_id"] = self.lock_id
        doc["lock_time"] = self.lock_time
        doc["created_on"] = self.created_on
        doc["updated_on"] = self.updated_on

        return doc

    @classmethod
    def from_dict(cls, doc_id: str, doc_dict: dict) -> SQLAuxiliary:
        """Create SQL row from MongoDB-like document dictionary."""
        # Store the full document content, only removing metadata fields that are stored separately
        clean_doc = dict(doc_dict)
        clean_doc.pop("_id", None)
        clean_doc.pop("lock_id", None)
        clean_doc.pop("lock_time", None)
        clean_doc.pop("created_on", None)
        clean_doc.pop("updated_on", None)

        # Store everything else (including "running_runner", etc.) as the full document
        # SQLAlchemy will handle JSON serialization automatically
        return cls(
            doc_id=doc_id,
            document=clean_doc,
            lock_id=doc_dict.get("lock_id"),
            lock_time=doc_dict.get("lock_time"),
        )


class SQLLock:
    """
    SQL-based locking mechanism similar to MongoLock.

    Provides document-level locking for SQL tables using atomic operations.
    """

    def __init__(
        self,
        session: Session,
        table_class,
        filter: dict[str, Any],
        update: dict[str, Any] | None = None,
        break_lock: bool = False,
        lock_id: str | None = None,
        sleep: int | None = None,
        max_wait: int = 600,
        get_locked_doc: bool = False,
    ):
        self.session = session
        self.table_class = table_class
        self.filter = filter
        self.update = update or {}
        self.break_lock = break_lock
        self.lock_id = lock_id or suuid()
        self.sleep = sleep
        self.max_wait = max_wait
        self.get_locked_doc = get_locked_doc

        self.locked_document = None
        self.unavailable_document = None
        self._locked_row_id = None  # Store the primary key for release operations
        self._update_on_release = {}
        self._delete_on_release = False

    @property
    def update_on_release(self) -> dict:
        return self._update_on_release

    @update_on_release.setter
    def update_on_release(self, value: dict):
        if self._delete_on_release:
            raise ValueError(
                "delete_on_release and update_on_release cannot be set simultaneously"
            )
        self._update_on_release = value

    @property
    def delete_on_release(self) -> bool:
        return self._delete_on_release

    @delete_on_release.setter
    def delete_on_release(self, value: bool):
        if self._update_on_release:
            raise ValueError(
                "delete_on_release and update_on_release cannot be set simultaneously"
            )
        self._delete_on_release = value

    def acquire(self) -> None:
        """Acquire the lock on the document using atomic UPDATE...RETURNING."""
        import time

        now = datetime.utcnow()

        # Build query conditions using MongoDB-to-SQL converter
        conditions = MongoToSQLConverter.convert_query(self.filter, self.table_class)

        # Add lock condition if not breaking locks
        if not self.break_lock:
            conditions.append(self.table_class.lock_id.is_(None))

        t0 = time.time()
        while True:
            try:
                # Prepare update values
                update_values = {
                    "lock_id": self.lock_id,
                    "lock_time": now,
                    "updated_on": now,
                }
                update_values.update(self.update)

                # Atomic UPDATE with RETURNING - this is the key for race condition safety
                update_stmt = (
                    update(self.table_class)
                    .where(and_(*conditions))
                    .values(**update_values)
                    .returning(self.table_class)
                    .execution_options(synchronize_session=False)
                )

                result = self.session.execute(update_stmt).first()

                if result:
                    # Successfully acquired lock atomically
                    sql_row = result[0]
                    self._locked_row_id = (
                        sql_row.id
                    )  # Store the primary key for release operations

                    # Convert to dictionary representation for compatibility with MongoLock
                    if hasattr(sql_row, "to_job_doc"):
                        self.locked_document = sql_row.to_job_doc().as_db_dict()
                    elif hasattr(sql_row, "to_flow_doc"):
                        self.locked_document = sql_row.to_flow_doc().as_db_dict()
                    else:
                        # For auxiliary documents, use the to_dict method
                        self.locked_document = sql_row.to_dict()
                    self.session.commit()
                    return

                elif self.get_locked_doc:
                    # Try to get the locked document for diagnostics
                    diag_conditions = MongoToSQLConverter.convert_query(
                        self.filter, self.table_class
                    )
                    locked_query = select(self.table_class).where(
                        and_(*diag_conditions)
                    )
                    locked_result = self.session.execute(locked_query).first()
                    if locked_result:
                        sql_row = locked_result[0]
                        # Convert to dictionary representation for compatibility with MongoLock
                        if hasattr(sql_row, "to_job_doc"):
                            self.unavailable_document = (
                                sql_row.to_job_doc().as_db_dict()
                            )
                        elif hasattr(sql_row, "to_flow_doc"):
                            self.unavailable_document = (
                                sql_row.to_flow_doc().as_db_dict()
                            )
                        else:
                            # For auxiliary documents, use the to_dict method
                            self.unavailable_document = sql_row.to_dict()
                    break

            except Exception as e:
                self.session.rollback()
                logger.debug(f"Lock acquisition failed: {e}")

            # Check if we should retry
            if self.sleep and (time.time() - t0) < self.max_wait:
                time.sleep(self.sleep)
            else:
                break

    def release(self, exc_type, exc_val, exc_tb) -> None:
        """Release the lock."""
        if not self.locked_document or self._locked_row_id is None:
            return

        try:
            if self._delete_on_release and exc_type is None:
                # Delete the document
                delete_stmt = delete(self.table_class).where(
                    and_(
                        self.table_class.id == self._locked_row_id,
                        self.table_class.lock_id == self.lock_id,
                    )
                )
                result = self.session.execute(delete_stmt)
                if result.rowcount == 0:
                    raise RuntimeError(
                        "Could not delete the locked document upon release"
                    )
            else:
                # Update or just release lock
                update_values = {
                    "lock_id": None,
                    "lock_time": None,
                    "updated_on": datetime.utcnow(),
                }

                # Apply additional updates if no exception occurred
                if exc_type is None and self._update_on_release:
                    additional_updates = {}
                    if "$set" in self._update_on_release:
                        additional_updates = self._update_on_release["$set"]
                    else:
                        additional_updates = self._update_on_release

                    # Special handling for SQLAuxiliary: update the JSON document content
                    if self.table_class.__name__ == "SQLAuxiliary":
                        if self.locked_document:
                            # locked_document already contains the parsed document with metadata
                            # Extract only the document fields (exclude metadata like _id, lock_id, etc.)
                            current_doc = {
                                k: v
                                for k, v in self.locked_document.items()
                                if k
                                not in [
                                    "_id",
                                    "lock_id",
                                    "lock_time",
                                    "created_on",
                                    "updated_on",
                                ]
                            }

                            # Apply MongoDB-style updates to the JSON document
                            for field, value in additional_updates.items():
                                current_doc[field] = value

                            # SQLAlchemy will handle JSON serialization automatically with custom encoders
                            update_values["document"] = current_doc
                    else:
                        # Convert MongoDB field names to SQL column names for regular tables
                        converted_updates = (
                            MongoToSQLConverter.convert_field_names_for_update(
                                additional_updates, self.table_class
                            )
                        )

                        # Handle JSON columns that need serialization
                        for key, value in converted_updates.items():
                            if key == "job" and isinstance(value, dict):
                                # The job column is JSON, needs to be serialized properly
                                # SQLAlchemy's JSON column type handles this automatically
                                converted_updates[key] = value
                            elif key in [
                                "parents",
                                "exec_config",
                                "resources",
                                "stored_data",
                                "ids",
                            ]:
                                # These are Text columns that store JSON strings
                                if isinstance(value, (dict, list)):
                                    converted_updates[key] = json.dumps(value)
                            elif key == "jobs" and isinstance(value, list):
                                # The jobs column is JSON but might need explicit handling in some cases
                                converted_updates[key] = value

                        update_values.update(converted_updates)

                # When break_lock was used, be more lenient with the WHERE condition
                if self.break_lock:
                    # For break_lock, only check the row ID to ensure updates are applied
                    where_clause = self.table_class.id == self._locked_row_id
                else:
                    # Normal case: check both row ID and lock ID for safety
                    where_clause = and_(
                        self.table_class.id == self._locked_row_id,
                        self.table_class.lock_id == self.lock_id,
                    )

                update_stmt = (
                    update(self.table_class).where(where_clause).values(**update_values)
                )

                result = self.session.execute(update_stmt)
                if result.rowcount == 0:
                    warnings.warn(
                        f"Could not release lock for document {self._locked_row_id}",
                        stacklevel=2,
                    )

            self.session.commit()

        except Exception:
            self.session.rollback()
            raise
        finally:
            self.locked_document = None
            self._locked_row_id = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.locked_document:
            self.release(exc_type, exc_val, exc_tb)


class SQLJobController(JobController):
    """
    SQL-based job controller that provides the same API as JobController.

    This class maintains the same functionality as the original JobController
    but interacts with an SQL database instead of MongoDB.
    """

    def __init__(
        self,
        db_url: str,
        jobstore: JobStore,
        project: Project | None = None,
        optional_jobstores: dict[str, JobStore] | None = None,
    ):
        """
        Initialize the SQL Job Controller.

        Parameters
        ----------
        db_url
            SQLAlchemy database URL (e.g., 'sqlite:///jobs.db')
        jobstore
            The JobStore containing the output of the jobflow Flows
        project
            The project where the Stores were defined
        optional_jobstores
            A dictionary of optional JobStores as defined in the project
        """
        self.db_url = db_url
        self.jobstore = jobstore
        self.optional_jobstores = optional_jobstores or {}
        self.project = project

        # Create engine and session factory
        self.engine = create_engine(db_url)
        self.SessionLocal = sessionmaker(bind=self.engine)

        # Create tables
        Base.metadata.create_all(bind=self.engine)

        # Connect jobstores
        self.jobstore.connect()
        for opt_js in self.optional_jobstores.values():
            opt_js.connect()

    @contextlib.contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """Get a database session."""
        session = self.SessionLocal()
        try:
            yield session
        finally:
            session.close()

    def close(self) -> None:
        """Close connections to all stores."""
        try:
            self.jobstore.close()
        except Exception:
            logger.exception("Error while closing the connection to the job store")

        for js_name, js in self.optional_jobstores.items():
            try:
                js.close()
            except Exception:
                logger.exception(
                    f"Error while closing the connection to the optional job store {js_name}"
                )

        # Close engine
        self.engine.dispose()

    def reset(
        self,
        reset_output: bool = False,
        max_limit: int = 25,
        validation: str | None = None,
    ) -> bool:
        """
        Reset the content of the queue database.

        Parameters
        ----------
        reset_output
            If True also reset the JobStore containing the outputs.
        max_limit
            Maximum number of Flows present in the DB. If number is larger
            the database a validation should be passed. Set 0 for not limit.
        validation
            A string representing today's date in the format YYYY-MM-DD.
            Required if the number of Flows to delete exceed max_limit.

        Returns
        -------
        bool
            True if the database was reset, False otherwise.
        """
        with self.get_session() as session:
            if max_limit:
                n_flows = session.query(SQLFlow).count()
                today = datetime.now().strftime("%Y-%m-%d")
                if n_flows >= max_limit and today != validation:
                    logger.warning(
                        f"The database contains {n_flows} flows and will not be reset. "
                        "Pass today's date in the YYYY-MM-DD format to validate the reset "
                        "or change the max_limit value."
                    )
                    return False

            if reset_output:
                self.jobstore.remove_docs({})
                for opt_jobstore in self.optional_jobstores.values():
                    opt_jobstore.remove_docs({})

            # Clear all tables
            session.execute(delete(SQLJob))
            session.execute(delete(SQLFlow))
            session.execute(delete(SQLAuxiliary))

            # Reinitialize auxiliary data
            session.add(
                SQLAuxiliary(doc_id="next_id", document={"value": 1})
            )  # Store as dict, not string
            session.add(
                SQLAuxiliary(doc_id="running_runner", document={"running_runner": None})
            )

            session.commit()

        return True

    def add_flow(
        self,
        flow: jobflow.Flow | jobflow.Job | list[jobflow.Job],
        worker: str,
        allow_external_references: bool = False,
        exec_config: ExecutionConfig | None = None,
        resources: dict | Any | None = None,  # QResources
        priority: int = 0,
        jobstore: str | None = None,
    ) -> list[str]:
        """
        Add a Flow to the database.

        Parameters
        ----------
        flow
            The Flow, Job, or list of Jobs to add.
        worker
            The worker name.
        allow_external_references
            Whether to allow external references.
        exec_config
            Execution configuration.
        resources
            Job resources.
        priority
            Job priority.
        jobstore
            Optional jobstore name.

        Returns
        -------
        list[str]
            List of db_ids of the added jobs.
        """
        from jobflow.core.flow import get_flow

        flow = get_flow(flow, allow_external_references=allow_external_references)

        jobs_list = list(flow.iterflow())
        job_dicts = []
        n_jobs = len(jobs_list)

        with self.get_session() as session:
            try:
                # Get the current next_id value from the document
                current_next_id_row = session.execute(
                    select(SQLAuxiliary).where(SQLAuxiliary.doc_id == "next_id")
                ).scalar_one()

                # With JSON column, document is already deserialized
                if isinstance(current_next_id_row.document, dict):
                    current_next_id = current_next_id_row.document.get("value", 1)
                else:
                    # Backward compatibility with old string format
                    current_next_id = int(current_next_id_row.document)

                first_id = current_next_id

                # Update next_id atomically
                update_result = session.execute(
                    update(SQLAuxiliary)
                    .where(SQLAuxiliary.doc_id == "next_id")
                    .values(document={"value": current_next_id + n_jobs})
                )

                if update_result.rowcount == 0:
                    raise RuntimeError("Could not update next_id")

                db_ids = []
                for (job, parents), db_id_int in zip(
                    jobs_list, range(first_id, first_id + n_jobs)
                ):
                    prefix = ""
                    if self.project and self.project.queue.db_id_prefix:
                        prefix = self.project.queue.db_id_prefix
                    db_id = f"{prefix}{db_id_int}"
                    db_ids.append(db_id)
                    job_dicts.append(
                        get_initial_job_doc_dict(
                            job,
                            parents,
                            db_id,
                            worker=worker,
                            exec_config=exec_config,
                            resources=resources,
                            priority=priority,
                        )
                    )

                flow_doc_dict = get_initial_flow_doc_dict(
                    flow, job_dicts, jobstore=jobstore
                )

                # Create and insert Flow
                flow_doc = FlowDoc.model_validate(flow_doc_dict)
                sql_flow = SQLFlow.from_flow_doc(flow_doc)
                session.add(sql_flow)

                # Create and insert Jobs
                for job_dict in job_dicts:
                    job_doc = JobDoc.model_validate(job_dict)
                    sql_job = SQLJob.from_job_doc(job_doc)
                    session.add(sql_job)

                session.commit()

                logger.info(f"Added flow ({flow.uuid}) with jobs: {flow.job_uuids}")

                return db_ids

            except Exception as e:
                session.rollback()
                # Check for duplicate key error on flows table
                from sqlalchemy.exc import IntegrityError

                if isinstance(e, IntegrityError):
                    error_str = str(e)
                    # Check if it's a unique constraint violation on the flows.uuid column
                    if (
                        "UNIQUE constraint failed: flows.uuid" in error_str
                        or "duplicate key value violates unique constraint" in error_str
                        and "flows" in error_str
                    ):
                        raise ValueError(
                            f"A duplicate key error happened while inserting the flow "
                            f"{flow.uuid} to the database. Make sure that the Flow was "
                            "not already inserted in the database"
                        ) from e
                    # Check for duplicate job db_id
                    elif (
                        "UNIQUE constraint failed: jobs.db_id" in error_str
                        or "duplicate key value violates unique constraint" in error_str
                        and "jobs" in error_str
                    ):
                        raise ValueError(
                            f"A duplicate key error happened while inserting jobs for flow "
                            f"{flow.uuid}. Some job db_ids may already exist in the database"
                        ) from e
                raise

    def checkout_job(
        self,
        query: dict | None = None,
        flow_uuid: str | None = None,
        sort: list[tuple[str, int] | str] | None = None,
    ) -> tuple[str, int] | None:
        """
        Check out one job using atomic UPDATE...RETURNING.

        This is the SQL equivalent of MongoDB's find_one_and_update for checkout.
        The atomic UPDATE...RETURNING prevents race conditions when multiple
        processes compete for the same job.

        Parameters
        ----------
        query
            Additional query filters.
        flow_uuid
            Specific flow UUID to limit checkout to.
        sort
            Sort order for job selection.

        Returns
        -------
        tuple[str, int] | None
            (uuid, index) of checked out job, or None if no job available.
        """
        with self.get_session() as session:
            # Build query conditions
            conditions = [SQLJob.state == JobState.READY.value]

            if query:
                for key, value in query.items():
                    if hasattr(SQLJob, key):
                        conditions.append(getattr(SQLJob, key) == value)

            if flow_uuid:
                # Get jobs in the specified flow
                flow_row = session.execute(
                    select(SQLFlow).where(SQLFlow.uuid == flow_uuid)
                ).scalar_one_or_none()

                if not flow_row:
                    return None

                flow_jobs = flow_row.jobs  # jobs field is now JSON column
                conditions.append(SQLJob.uuid.in_(flow_jobs))

            try:
                # Atomic checkout using UPDATE...RETURNING
                # This is the key for race condition safety - equivalent to MongoDB's find_one_and_update
                subquery = (
                    select(SQLJob.id)
                    .where(and_(*conditions))
                    .order_by(
                        SQLJob.priority.desc() if sort is None else None,
                        SQLJob.created_on.asc() if sort is None else None,
                    )
                    .limit(1)
                )

                update_stmt = (
                    update(SQLJob)
                    .where(SQLJob.id.in_(subquery))
                    .values(
                        state=JobState.CHECKED_OUT.value,
                        updated_on=datetime.utcnow(),
                    )
                    .returning(SQLJob.uuid, SQLJob.index)
                    .execution_options(synchronize_session=False)
                )

                result = session.execute(update_stmt).first()

                if result:
                    reserved_uuid, reserved_index = result

                    # Update flow state atomically if it's in READY state
                    flow_update_stmt = (
                        update(SQLFlow)
                        .where(
                            and_(
                                SQLFlow.jobs.contains(f'"{reserved_uuid}"'),
                                SQLFlow.state == FlowState.READY.value,
                            )
                        )
                        .values(
                            state=FlowState.RUNNING.value,
                            updated_on=datetime.utcnow(),
                        )
                    )

                    session.execute(flow_update_stmt)
                    session.commit()

                    return reserved_uuid, reserved_index

                # No job was available for checkout
                return None

            except Exception as e:
                session.rollback()
                logger.debug(f"Job checkout failed: {e}")
                return None

    @contextlib.contextmanager
    def lock_flow_by_job_uuid(self, job_uuid: str) -> Generator[SQLLock, None, None]:
        """Lock a flow that contains the specified job UUID."""
        with self.get_session() as session:
            # Find the flow containing this job
            # Use limit(1).first() to match MongoDB find_one() behavior
            result = session.execute(
                select(SQLFlow).where(SQLFlow.jobs.contains(f'"{job_uuid}"')).limit(1)
            ).first()
            flow_row = result[0] if result else None

            if not flow_row:
                raise ValueError(f"No flow found containing job {job_uuid}")

            # Create lock for this flow
            lock = SQLLock(
                session=session,
                table_class=SQLFlow,
                filter={"uuid": flow_row.uuid},
                get_locked_doc=True,
            )

            with lock:
                yield lock

    def complete_job(
        self, job_doc: dict, local_path: Path | str, store: JobStore
    ) -> bool:
        """
        Complete a job by processing its output and updating the database.

        Parameters
        ----------
        job_doc
            Dictionary representation of the job document.
        local_path
            Path to the local job output directory.
        store
            JobStore to use for storing output.

        Returns
        -------
        bool
            True if job was completed successfully.
        """
        from monty.json import MontyDecoder
        from monty.serialization import loadfn

        from jobflow_remote.jobs.data import OUT_FILENAME
        from jobflow_remote.remote.data import (
            get_remote_store,
            get_remote_store_filenames,
            update_store,
        )

        # Use SQL lock instead of MongoDB lock
        with self.lock_flow_by_job_uuid(job_doc["uuid"]) as flow_lock:
            if flow_lock.locked_document:
                local_path = Path(local_path)
                out_path = local_path / OUT_FILENAME
                host_flow_id = job_doc["job"]["hosts"][-1]

                if not out_path.exists():
                    msg = (
                        f"The output file {OUT_FILENAME} was not present in the download "
                        f"folder {local_path} and it is required to complete the job"
                    )
                    self.checkin_job(
                        job_doc, flow_lock.locked_document, response=None, error=msg
                    )
                    self.update_flow_state(host_flow_id)
                    return True

                # Load output
                out = loadfn(out_path, cls=None)
                decoder = MontyDecoder()
                doc_update = {"start_time": decoder.process_decoded(out["start_time"])}

                end_time = decoder.process_decoded(out.get("end_time"))
                if end_time:
                    doc_update["end_time"] = end_time

                error = out.get("error")
                if error:
                    self.checkin_job(
                        job_doc,
                        flow_lock.locked_document,
                        response=None,
                        error=error,
                        doc_update=doc_update,
                    )
                    self.update_flow_state(host_flow_id)
                    return True

                response = out.get("response")
                if not response:
                    msg = (
                        f"The output file {OUT_FILENAME} was downloaded, but it does "
                        "not contain the response. The job was likely killed "
                        "before completing"
                    )
                    self.checkin_job(
                        job_doc,
                        flow_lock.locked_document,
                        response=None,
                        error=msg,
                        doc_update=doc_update,
                    )
                    self.update_flow_state(host_flow_id)
                    return True

                # Check for required store files
                config_dict = self.project.remote_jobstore if self.project else {}
                required_store_files = get_remote_store_filenames(
                    store, config_dict=config_dict
                )
                for store_file in required_store_files:
                    if not (local_path / store_file).exists():
                        msg = (
                            "No explicit error raised during the remote execution, but the output "
                            f"store file {store_file} is missing in the downloaded folder {local_path}. "
                            "The file was probably not created in the remote folder during the "
                            "execution but is needed to proceed."
                        )
                        self.checkin_job(
                            job_doc,
                            flow_lock.locked_document,
                            response=None,
                            error=msg,
                            doc_update=doc_update,
                        )
                        self.update_flow_state(host_flow_id)
                        return True

                # Update store
                remote_store = get_remote_store(store, local_path, config_dict)

                update_store(store, remote_store, job_doc["db_id"])

                self.checkin_job(
                    job_doc,
                    flow_lock.locked_document,
                    response=response,
                    doc_update=doc_update,
                )
                self.update_flow_state(host_flow_id)
                return True

        return False

    def checkin_job(
        self,
        job_doc: dict,
        flow_dict: dict,
        response: dict | None,
        error: str | None = None,
        doc_update: dict | None = None,
    ) -> int:
        """
        Check in a job after completion or failure.

        Parameters
        ----------
        job_doc
            Dictionary representation of the job document.
        flow_dict
            Dictionary representation of the flow document.
        response
            Job response data.
        error
            Error message if job failed.
        doc_update
            Additional updates to apply to job document.

        Returns
        -------
        int
            Number of jobs that were modified (including children).
        """
        stored_data = None
        queue_out = None
        queue_err = None

        if response is None:
            new_state = JobState.FAILED.value
        else:
            new_state = JobState.COMPLETED.value

            # Handle dynamic responses (replace, addition, detour)
            stop_generated = response.get("stop_children", False) or response.get(
                "stop_jobflow", False
            )

            if response.get("replace") is not None:
                self._append_flow(
                    job_doc,
                    flow_dict,
                    response["replace"],
                    response_type=DynamicResponseType.REPLACE,
                    worker=job_doc["worker"],
                    exec_config=job_doc.get("exec_config"),
                    resources=job_doc.get("resources"),
                    priority=job_doc.get("priority", 0),
                    stopped=stop_generated,
                )

            if response.get("addition") is not None:
                self._append_flow(
                    job_doc,
                    flow_dict,
                    response["addition"],
                    response_type=DynamicResponseType.ADDITION,
                    worker=job_doc["worker"],
                    exec_config=job_doc.get("exec_config"),
                    resources=job_doc.get("resources"),
                    priority=job_doc.get("priority", 0),
                    stopped=stop_generated,
                )

            if response.get("detour") is not None:
                self._append_flow(
                    job_doc,
                    flow_dict,
                    response["detour"],
                    response_type=DynamicResponseType.DETOUR,
                    worker=job_doc["worker"],
                    exec_config=job_doc.get("exec_config"),
                    resources=job_doc.get("resources"),
                    priority=job_doc.get("priority", 0),
                    stopped=stop_generated,
                )

            if response.get("stored_data"):
                stored_data = response["stored_data"]

            if response.get("stop_children"):
                self.stop_children(job_doc["uuid"])

            if response.get("stop_jobflow"):
                self.stop_jobflow(job_uuid=job_doc["uuid"])

        if not doc_update:
            doc_update = {}

        doc_update.update(
            {
                "state": new_state,
                "stored_data": json.dumps(stored_data) if stored_data else None,
                "error": error,
                "remote_queue_out": queue_out,
                "remote_queue_err": queue_err,
                "updated_on": datetime.utcnow(),
            }
        )

        with self.get_session() as session:
            # Update job
            result = session.execute(
                update(SQLJob)
                .where(
                    and_(
                        SQLJob.uuid == job_doc["uuid"], SQLJob.index == job_doc["index"]
                    )
                )
                .values(**doc_update)
            )

            if result.rowcount == 0:
                raise RuntimeError(
                    f"The job {job_doc['uuid']} index {job_doc['index']} has not been updated in the database"
                )

            session.commit()

        # Refresh children
        job_uuids = flow_dict["jobs"]
        return len(self.refresh_children(job_uuids)) + 1

    def refresh_children(self, job_uuids: list[str]) -> list[str]:
        """
        Set the state of Jobs children to READY following the completion of a Job.

        Parameters
        ----------
        job_uuids
            List of Jobs uuids belonging to a Flow.

        Returns
        -------
        list[str]
            List of db_ids of modified Jobs.
        """
        with self.get_session() as session:
            # Get all jobs in the flow, sorted by index
            flow_jobs = (
                session.execute(
                    select(SQLJob)
                    .where(SQLJob.uuid.in_(job_uuids))
                    .order_by(SQLJob.index.asc())
                )
                .scalars()
                .all()
            )

            # Create mapping with highest index for each uuid
            jobs_mapping = {}
            for job in flow_jobs:
                jobs_mapping[job.uuid] = job

            # Find jobs that can be moved to READY
            to_ready = []
            for job in jobs_mapping.values():
                if job.state == JobState.WAITING.value:
                    # Parse parents
                    parents = json.loads(job.parents) if job.parents else []

                    # Check if all parents are completed
                    all_parents_completed = True
                    for parent_uuid in parents:
                        parent_job = jobs_mapping.get(parent_uuid)
                        if (
                            not parent_job
                            or parent_job.state != JobState.COMPLETED.value
                        ):
                            # Check for allowed failure states based on job config
                            job_data = job.job  # job field is now JSON column
                            on_missing_ref = job_data.get("config", {}).get(
                                "on_missing_references"
                            )

                            if (
                                parent_job
                                and on_missing_ref == jobflow.OnMissing.NONE.value
                                and parent_job.state
                                in [JobState.FAILED.value, JobState.USER_STOPPED.value]
                            ):
                                continue

                            all_parents_completed = False
                            break

                    if all_parents_completed:
                        to_ready.append(job.db_id)

            # Update jobs to READY state
            if to_ready:
                session.execute(
                    update(SQLJob)
                    .where(SQLJob.db_id.in_(to_ready))
                    .values(state=JobState.READY.value, updated_on=datetime.utcnow())
                )
                session.commit()

            return to_ready

    def _append_flow(
        self,
        job_doc: dict,
        flow_dict: dict,
        new_flow_dict: dict,
        worker: str,
        response_type: DynamicResponseType,
        exec_config: ExecutionConfig | None = None,
        resources: dict | Any | None = None,  # QResources
        priority: int = 0,
        stopped: bool = False,
    ) -> None:
        """
        Append a new Flow to an existing one as a child of a specific Job.

        Parameters
        ----------
        job_doc
            The dictionary representation of the JobDoc of the Job to which
             the new Flow will be appended.
        flow_dict
            The dictionary of the original Flow.
        new_flow_dict
            The dictionary of the new Flow.
        worker
            The default worker applied to the newly created Jobs, if not
            overridden by specific Job configurations.
        response_type
            Type of response.
        exec_config
            ExecutionConfig inherited from the generating Job, if not overridden
            by specific Job updates.
        resources
            Resources inherited from the generating Job, if not overridden
            by specific Job updates.
        priority
            Priority inherited from the generating Job.
        stopped
            If True the generated Jobs will be set in the STOPPED state.
        """
        import jobflow
        from monty.json import MontyDecoder

        decoder = MontyDecoder()

        def deserialize_partial_flow(in_dict: dict):
            """
            Recursively deserialize a Flow dictionary, avoiding the deserialization
            of all the elements that may require external packages.
            """
            if in_dict.get("@class") == "Flow":
                jobs = [deserialize_partial_flow(d) for d in in_dict.get("jobs", [])]
                flow_init = {
                    k: v
                    for k, v in in_dict.items()
                    if k not in ("@module", "@class", "@version", "jobs")
                }
                flow_init["jobs"] = jobs
                return jobflow.Flow(**flow_init)
            # if it is not a Flow, should be a Job
            job_init = {
                k: v
                for k, v in in_dict.items()
                if k not in ("@module", "@class", "@version")
            }
            job_init["config"] = decoder.process_decoded(job_init["config"])
            return jobflow.Job(**job_init)

        # Recursive deserialize the Flow without deserializing function and
        # arguments to take advantage of standard Flow/Job methods.
        new_flow = deserialize_partial_flow(new_flow_dict)

        # get job parents. Job parents are identified only by their uuid.
        if response_type == DynamicResponseType.REPLACE:
            job_parents = job_doc["parents"]
        else:
            job_parents = [job_doc["uuid"]]

        # add new jobs
        jobs_list = list(new_flow.iterflow())
        n_new_jobs = len(jobs_list)

        with self.get_session() as session:
            try:
                # Get the current next_id value from the document
                current_next_id_row = session.execute(
                    select(SQLAuxiliary).where(SQLAuxiliary.doc_id == "next_id")
                ).scalar_one()

                # With JSON column, document is already deserialized
                if isinstance(current_next_id_row.document, dict):
                    current_next_id = current_next_id_row.document.get("value", 1)
                else:
                    # Backward compatibility with old string format
                    current_next_id = int(current_next_id_row.document)

                first_id = current_next_id

                # Update next_id atomically
                update_result = session.execute(
                    update(SQLAuxiliary)
                    .where(SQLAuxiliary.doc_id == "next_id")
                    .values(document={"value": current_next_id + n_new_jobs})
                )

                if update_result.rowcount == 0:
                    raise RuntimeError("Could not update next_id")

                # Create new job documents
                job_dicts = []
                ids_to_add = []
                parents_to_update = {}

                for (job, parents), db_id_int in zip(
                    jobs_list, range(first_id, first_id + n_new_jobs)
                ):
                    prefix = ""
                    if self.project and self.project.queue.db_id_prefix:
                        prefix = self.project.queue.db_id_prefix
                    db_id = f"{prefix}{db_id_int}"

                    # inherit the parents of the job to which we are appending
                    parents = parents if parents else job_parents
                    # Ensure parents is always a list, never None
                    if parents is None:
                        parents = []

                    init_job_doc = get_initial_job_doc_dict(
                        job,
                        parents,
                        db_id,
                        worker=worker,
                        exec_config=exec_config,
                        resources=resources,
                        priority=priority,
                    )

                    if stopped:
                        init_job_doc["state"] = JobState.STOPPED.value

                    job_dicts.append(init_job_doc)
                    # Create proper nested parents structure: {uuid: {index: [parent_uuids]}}
                    if job.uuid not in parents_to_update:
                        parents_to_update[job.uuid] = {}
                    parents_to_update[job.uuid][str(job.index)] = parents
                    ids_to_add.append((db_id, job.uuid, job.index))

                # Update flow document
                flow_row = session.execute(
                    select(SQLFlow).where(SQLFlow.uuid == flow_dict["uuid"])
                ).scalar_one()

                # Add new job UUIDs to the flow's jobs list, ensuring uniqueness
                current_jobs = flow_row.jobs  # Already a list due to JSON column
                # Use a set to ensure each UUID appears only once
                unique_job_uuids = set(current_jobs)
                unique_job_uuids.update(new_flow.job_uuids)
                updated_jobs = list(unique_job_uuids)

                # Update flow parents and ids
                current_parents = (
                    json.loads(flow_row.parents) if flow_row.parents else {}
                )
                # Merge parents properly to handle existing job UUIDs
                for job_uuid, job_indices in parents_to_update.items():
                    if job_uuid not in current_parents:
                        current_parents[job_uuid] = {}
                    current_parents[job_uuid].update(job_indices)

                current_ids = json.loads(flow_row.ids) if flow_row.ids else []
                current_ids.extend(ids_to_add)

                # Update the flow
                session.execute(
                    update(SQLFlow)
                    .where(SQLFlow.uuid == flow_dict["uuid"])
                    .values(
                        jobs=updated_jobs,
                        parents=json.dumps(current_parents),
                        ids=json.dumps(current_ids),
                        updated_on=datetime.utcnow(),
                    )
                )

                # Insert new jobs
                for job_dict in job_dicts:
                    job_doc = JobDoc.model_validate(job_dict)
                    sql_job = SQLJob.from_job_doc(job_doc)
                    session.add(sql_job)

                # Handle DETOUR type - update children's parents
                if response_type == DynamicResponseType.DETOUR:
                    # Find leaf jobs (jobs with no children) in the new flow
                    leaf_uuids = []
                    for job in new_flow.jobs:
                        has_children = False
                        for other_job in new_flow.jobs:
                            if job.uuid in (other_job.parents or []):
                                has_children = True
                                break
                        if not has_children:
                            leaf_uuids.append(job.uuid)

                    # Update jobs that have the original job as parent
                    if leaf_uuids:
                        # Find jobs that have job_doc["uuid"] as parent and add leaf_uuids to their parents
                        jobs_to_update = (
                            session.execute(
                                select(SQLJob).where(
                                    SQLJob.parents.contains(f'"{job_doc["uuid"]}"')
                                )
                            )
                            .scalars()
                            .all()
                        )

                        for job_to_update in jobs_to_update:
                            current_parents_list = (
                                json.loads(job_to_update.parents)
                                if job_to_update.parents
                                else []
                            )
                            updated_parents_list = current_parents_list + leaf_uuids
                            session.execute(
                                update(SQLJob)
                                .where(SQLJob.id == job_to_update.id)
                                .values(parents=json.dumps(updated_parents_list))
                            )

                session.commit()
                logger.info(
                    f"Appended flow ({new_flow.uuid}) with jobs: {new_flow.job_uuids}"
                )

            except Exception as e:
                session.rollback()
                # Check for duplicate key error when appending jobs
                from sqlalchemy.exc import IntegrityError

                if isinstance(e, IntegrityError):
                    error_str = str(e)
                    # Check for duplicate job db_id or uuid
                    if (
                        "UNIQUE constraint failed: jobs.db_id" in error_str
                        or "duplicate key value violates unique constraint" in error_str
                        and "jobs" in error_str
                    ):
                        raise ValueError(
                            f"A duplicate key error happened while appending jobs for flow "
                            f"{new_flow.uuid}. Some job db_ids may already exist in the database"
                        ) from e
                raise

    def stop_children(self, job_uuid: str) -> int:
        """
        Stop the direct children of a Job in the WAITING state.

        Parameters
        ----------
        job_uuid
            The uuid of the Job.

        Returns
        -------
        int
            The number of modified Jobs.
        """
        with self.get_session() as session:
            # Find jobs that have this uuid as parent
            result = session.execute(
                update(SQLJob)
                .where(
                    and_(
                        SQLJob.parents.contains(f'"{job_uuid}"'),
                        SQLJob.state.in_(
                            [JobState.WAITING.value, JobState.READY.value]
                        ),
                        SQLJob.lock_id.is_(None),
                    )
                )
                .values(state=JobState.STOPPED.value, updated_on=datetime.utcnow())
            )

            session.commit()
            return result.rowcount

    def stop_jobflow(self, job_uuid: str = None, flow_uuid: str = None) -> int:
        """
        Stop all the WAITING Jobs in a Flow.

        Parameters
        ----------
        job_uuid
            The uuid of Job to identify the Flow. Incompatible with flow_uuid.
        flow_uuid
            The Flow uuid. Incompatible with job_uuid.

        Returns
        -------
        int
            The number of modified Jobs.
        """
        if job_uuid is None and flow_uuid is None:
            raise ValueError("Either job_uuid or flow_uuid must be set.")

        if job_uuid is not None and flow_uuid is not None:
            raise ValueError("Only one of job_uuid and flow_uuid should be set.")

        with self.get_session() as session:
            if job_uuid:
                # Find flow containing this job
                # Use limit(1).first() to match MongoDB find_one() behavior
                result = session.execute(
                    select(SQLFlow)
                    .where(SQLFlow.jobs.contains(f'"{job_uuid}"'))
                    .limit(1)
                ).first()
                flow_row = result[0] if result else None

                if not flow_row:
                    return 0

                job_uuids = flow_row.jobs
            else:
                # Use provided flow_uuid
                flow_row = session.execute(
                    select(SQLFlow).where(SQLFlow.uuid == flow_uuid)
                ).scalar_one_or_none()

                if not flow_row:
                    return 0

                job_uuids = flow_row.jobs

            # Stop all waiting/ready jobs in the flow
            result = session.execute(
                update(SQLJob)
                .where(
                    and_(
                        SQLJob.uuid.in_(job_uuids),
                        SQLJob.state.in_(
                            [JobState.WAITING.value, JobState.READY.value]
                        ),
                        SQLJob.lock_id.is_(None),
                    )
                )
                .values(state=JobState.STOPPED.value, updated_on=datetime.utcnow())
            )

            session.commit()
            return result.rowcount

    def update_flow_state(
        self,
        flow_uuid: str,
        updated_states: dict[str, dict[int, JobState]] | None = None,
    ) -> FlowState:
        """
        Update the state of a Flow in the DB based on the Job's states.

        Parameters
        ----------
        flow_uuid
            The uuid of the Flow to update.
        updated_states
            Optional dict mapping job_uuid -> {index -> JobState} for jobs that have
            pending state changes not yet reflected in the database.

        Returns
        -------
        FlowState
            The state set for the Flow.
        """
        with self.get_session() as session:
            # Get all jobs in the flow
            flow_row = session.execute(
                select(SQLFlow).where(SQLFlow.uuid == flow_uuid)
            ).scalar_one_or_none()

            if not flow_row:
                raise ValueError(f"Flow {flow_uuid} not found")

            job_uuids = flow_row.jobs

            # Get job states
            jobs = session.execute(
                select(SQLJob.uuid, SQLJob.index, SQLJob.state, SQLJob.parents).where(
                    SQLJob.uuid.in_(job_uuids)
                )
            ).all()

            if not jobs:
                return FlowState.READY

            # Convert to JobState objects, considering updated_states
            job_states = []
            for job in jobs:
                if (
                    updated_states
                    and job.uuid in updated_states
                    and job.index in updated_states[job.uuid]
                ):
                    # Check if job is marked for deletion (None value)
                    pending_state = updated_states[job.uuid][job.index]
                    if pending_state is not None:
                        # Use the pending updated state
                        job_states.append(pending_state)
                    # If pending_state is None, skip this job (it's being deleted)
                else:
                    # Use the current database state
                    job_states.append(JobState(job.state))

            # Find leaf jobs (jobs with no children)
            jobs_with_children = set()
            for job in jobs:
                parents = json.loads(job.parents) if job.parents else []
                jobs_with_children.update(parents)

            # Build leaf states considering updated_states
            leaf_states = []
            for job in jobs:
                if job.uuid not in jobs_with_children:
                    if (
                        updated_states
                        and job.uuid in updated_states
                        and job.index in updated_states[job.uuid]
                    ):
                        # Check if job is marked for deletion (None value)
                        pending_state = updated_states[job.uuid][job.index]
                        if pending_state is not None:
                            # Use the pending updated state
                            leaf_states.append(pending_state)
                        # If pending_state is None, skip this job (it's being deleted)
                    else:
                        # Use the current database state
                        leaf_states.append(JobState(job.state))

            # Determine flow state
            flow_state = FlowState.from_jobs_states(
                jobs_states=job_states, leaf_states=leaf_states
            )

            # Update flow state if it changed
            current_state = FlowState(flow_row.state)
            if current_state != flow_state:
                session.execute(
                    update(SQLFlow)
                    .where(SQLFlow.uuid == flow_uuid)
                    .values(
                        state=flow_state.value,
                        updated_on=datetime.utcnow(),
                    )
                )
                session.commit()

            return flow_state

    def get_jobs_info_query(self, query: dict = None, **kwargs) -> list[JobInfo]:
        """
        Get a list of JobInfo by specifying a query.

        Parameters
        ----------
        query
            A dictionary with the filters to apply.
        **kwargs
            Additional query parameters.

        Returns
        -------
        list[JobInfo]
            The list of JobInfo matching the query.
        """
        query = query or {}
        query.update(kwargs)

        with self.get_session() as session:
            # Convert MongoDB-style query to SQL conditions using converter
            conditions = MongoToSQLConverter.convert_query(query, SQLJob)

            # Execute query
            query_stmt = select(SQLJob)
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            jobs = session.execute(query_stmt).scalars().all()

            # Convert to JobInfo objects
            job_infos = []
            for job in jobs:
                job_doc = job.to_job_doc()
                job_info = JobInfo.from_query_output(job_doc.as_db_dict())
                job_infos.append(job_info)

            return job_infos

    def get_jobs_info(
        self,
        custom_query: dict | None = None,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        job_index: int | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        locked: bool = False,
        sort: list[tuple[str, int] | str] | None = None,
        limit: int = 0,
        skip: int = 0,
    ) -> list[JobInfo]:
        """
        Get a list of JobInfo based on filter parameters.

        Parameters
        ----------
        custom_query
            A generic query. Keys must not overlap with other specified query options.
        job_ids
            List of Job uuids.
        db_ids
            List of db_ids.
        flow_ids
            List of Flow uuids.
        job_index
            Job index.
        states
            List of JobStates.
        start_date
            Filter Jobs updated after this date.
        end_date
            Filter Jobs updated before this date.
        name
            Pattern for the Job name.
        metadata
            A dictionary with metadata to match.
        workers
            List of worker names.
        locked
            If True, query only locked Jobs. If False, query any job (no lock filtering).
        sort
            Sort specification. Accepts:
            - [("field", direction), ...] - explicit direction (1=asc, -1=desc)
            - ["field", ...] - field names only (defaults to ascending)
            - Mixed formats: [("field", -1), "other_field"]
        limit
            Maximum number of entries to retrieve.
        skip
            Number of entries to skip.

        Returns
        -------
        list[JobInfo]
            List of JobInfo objects.
        """
        # job_ids is already in the correct format: either a single tuple (uuid, index)
        # or a list of tuples [(uuid, index), ...], so pass it directly to _build_query_job
        job_ids_tuples = job_ids

        # Build the MongoDB-style query using existing method
        query_dict = self._build_query_job(
            job_ids=job_ids_tuples,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            locked=locked,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
        )

        with self.get_session() as session:
            # Convert MongoDB-style query to SQL conditions
            conditions = MongoToSQLConverter.convert_query(query_dict, SQLJob)

            # Handle job_index separately if provided
            if job_index is not None:
                conditions.append(SQLJob.index == job_index)

            # Build query
            query_stmt = select(SQLJob)
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            # Sorting - normalize sort parameters to handle both MongoDB formats
            normalized_sort = self._normalize_sort_params(sort)
            if normalized_sort:
                order_clauses = []
                for field, direction in normalized_sort:
                    if hasattr(SQLJob, field):
                        attr = getattr(SQLJob, field)
                        if direction == pymongo.DESCENDING or direction == -1:
                            order_clauses.append(attr.desc())
                        else:
                            order_clauses.append(attr.asc())
                if order_clauses:
                    query_stmt = query_stmt.order_by(*order_clauses)

            # Skip and Limit
            if skip > 0:
                query_stmt = query_stmt.offset(skip)
            if limit > 0:
                query_stmt = query_stmt.limit(limit)

            # Execute query
            jobs = session.execute(query_stmt).scalars().all()

            # Convert to JobInfo objects
            job_infos = []
            for job in jobs:
                job_doc = job.to_job_doc()
                job_info = JobInfo.from_query_output(job_doc.as_db_dict())
                job_infos.append(job_info)

            return job_infos

    def count_jobs(
        self,
        query: dict | None = None,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        job_index: int | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        locked: bool = False,
    ) -> int:
        """
        Count jobs based on filter parameters.

        Parameters
        ----------
        query
            Custom query parameters.
        job_ids
            List of Job uuids.
        db_ids
            List of db_ids.
        flow_ids
            List of Flow uuids.
        job_index
            Job index.
        states
            List of JobStates.
        start_date
            Filter Jobs updated after this date.
        end_date
            Filter Jobs updated before this date.
        name
            Pattern for the Job name.
        metadata
            A dictionary with metadata to match.
        workers
            List of worker names.
        locked
            If True, query only locked Jobs. If False, query only not locked Jobs.

        Returns
        -------
        int
            Number of jobs matching the criteria.
        """
        # job_ids is already in the correct format: either a single tuple (uuid, index)
        # or a list of tuples [(uuid, index), ...], so pass it directly to _build_query_job
        job_ids_tuples = job_ids

        # Build the MongoDB-style query using existing method
        query_dict = self._build_query_job(
            job_ids=job_ids_tuples,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            locked=locked,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=query,
        )

        with self.get_session() as session:
            # Convert MongoDB-style query to SQL conditions
            conditions = MongoToSQLConverter.convert_query(query_dict, SQLJob)

            # Handle job_index separately if provided
            if job_index is not None:
                conditions.append(SQLJob.index == job_index)

            # Build count query
            from sqlalchemy import func

            query_stmt = select(func.count(SQLJob.id))
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            # Execute count query
            result = session.execute(query_stmt).scalar()
            return result or 0

    def get_jobs_doc_query(self, query: dict = None, **kwargs) -> list[JobDoc]:
        """
        Get a list of JobDoc by specifying a query.

        Parameters
        ----------
        query
            A dictionary with the filters to apply.
        **kwargs
            Additional query parameters.

        Returns
        -------
        list[JobDoc]
            The list of JobDoc matching the query.
        """
        query = query or {}
        query.update(kwargs)

        with self.get_session() as session:
            # Build query conditions
            conditions = []
            for key, value in query.items():
                if hasattr(SQLJob, key):
                    if key == "state":
                        if isinstance(value, JobState):
                            conditions.append(SQLJob.state == value.value)
                        elif isinstance(value, dict) and "$in" in value:
                            states = [
                                s.value if isinstance(s, JobState) else s
                                for s in value["$in"]
                            ]
                            conditions.append(SQLJob.state.in_(states))
                        else:
                            conditions.append(SQLJob.state == value)
                    else:
                        conditions.append(getattr(SQLJob, key) == value)

            # Execute query
            query_stmt = select(SQLJob)
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            jobs = session.execute(query_stmt).scalars().all()

            # Convert to JobDoc objects
            return [job.to_job_doc() for job in jobs]

    def get_jobs_doc(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        job_index: int | None = None,
        states: JobState | list[JobState] | None = None,
        locked: bool = False,
        sort: list[tuple[str, int] | str] | None = None,
        limit: int = 0,
    ) -> list[JobDoc]:
        """
        Get a list of JobDoc based on filter parameters.

        Parameters
        ----------
        job_ids
            List of Job uuids.
        db_ids
            List of db_ids.
        flow_ids
            List of Flow uuids.
        job_index
            Job index.
        states
            List of JobStates.
        locked
            If True, query only locked Jobs. If False, query any job (no lock filtering).
        sort
            List of (field, direction) to sort by.
        limit
            Maximum number of entries to retrieve.

        Returns
        -------
        list[JobDoc]
            List of JobDoc objects.
        """
        # Use the same query building pattern as get_jobs_info and count_jobs
        query_dict = self._build_query_job(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            locked=locked,
            start_date=None,
            end_date=None,
            name=None,
            metadata=None,
            workers=None,
            custom_query=None,
        )

        # Add job_index filter if specified
        if job_index is not None:
            query_dict["index"] = job_index

        with self.get_session() as session:
            # Convert MongoDB-style query to SQL conditions
            conditions = MongoToSQLConverter.convert_query(query_dict, SQLJob)

            # Build the SQL query
            query_stmt = select(SQLJob)
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            # Sorting - normalize sort parameters to handle both MongoDB formats
            normalized_sort = self._normalize_sort_params(sort)
            if normalized_sort:
                order_clauses = []
                for field, direction in normalized_sort:
                    if hasattr(SQLJob, field):
                        attr = getattr(SQLJob, field)
                        if direction == pymongo.DESCENDING or direction == -1:
                            order_clauses.append(attr.desc())
                        else:
                            order_clauses.append(attr.asc())
                if order_clauses:
                    query_stmt = query_stmt.order_by(*order_clauses)

            # Limit
            if limit > 0:
                query_stmt = query_stmt.limit(limit)

            # Execute query
            jobs = session.execute(query_stmt).scalars().all()

            # Convert to JobDoc objects
            return [job.to_job_doc() for job in jobs]

    def get_job_info(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
    ) -> JobInfo | None:
        """
        Get information about a single Job.

        Parameters
        ----------
        job_id
            The uuid of the Job.
        db_id
            The db_id of the Job.
        job_index
            The index of the Job. Used only in conjunction with job_id.

        Returns
        -------
        JobInfo | None
            The JobInfo object or None if not found.
        """
        if job_id is None and db_id is None:
            raise ValueError("Either job_id or db_id must be specified")

        with self.get_session() as session:
            conditions = []

            if job_id is not None:
                conditions.append(SQLJob.uuid == job_id)
                if job_index is not None:
                    conditions.append(SQLJob.index == job_index)

            if db_id is not None:
                conditions.append(SQLJob.db_id == db_id)

            query_stmt = select(SQLJob).where(and_(*conditions))

            # If job_id is provided without job_index, get the job with highest index
            if job_id is not None and job_index is None:
                query_stmt = query_stmt.order_by(SQLJob.index.desc())
                job = session.execute(query_stmt).first()
                if job:
                    job = job[0]  # Extract the SQLJob from the Row tuple
            else:
                # Use limit(1).first() to match MongoDB find_one() behavior
                # In case multiple documents match, take the first one
                result = session.execute(query_stmt.limit(1)).first()
                job = result[0] if result else None

            if job:
                job_doc = job.to_job_doc()
                return JobInfo.from_query_output(job_doc.as_db_dict())

            return None

    def get_job_doc(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
    ) -> JobDoc | None:
        """
        Get a single JobDoc.

        Parameters
        ----------
        job_id
            The uuid of the Job.
        db_id
            The db_id of the Job.
        job_index
            The index of the Job. Used only in conjunction with job_id.

        Returns
        -------
        JobDoc | None
            The JobDoc object or None if not found.
        """
        if job_id is None and db_id is None:
            raise ValueError("Either job_id or db_id must be specified")

        with self.get_session() as session:
            conditions = []

            if job_id is not None:
                conditions.append(SQLJob.uuid == job_id)
                if job_index is not None:
                    conditions.append(SQLJob.index == job_index)

            if db_id is not None:
                conditions.append(SQLJob.db_id == db_id)

            query_stmt = select(SQLJob).where(and_(*conditions))

            # If job_id is provided without job_index, get the job with highest index
            if job_id is not None and job_index is None:
                query_stmt = query_stmt.order_by(SQLJob.index.desc())
                job = session.execute(query_stmt).first()
                if job:
                    job = job[0]  # Extract the SQLJob from the Row tuple
            else:
                # Use limit(1).first() to match MongoDB find_one() behavior
                # In case multiple documents match, take the first one
                result = session.execute(query_stmt.limit(1)).first()
                job = result[0] if result else None

            if job:
                return job.to_job_doc()

            return None

    def get_flows_info(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: FlowState | list[FlowState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        locked: bool = False,
        sort: list[tuple[str, int] | str] | None = None,
        limit: int = 0,
        skip: int = 0,
        full: bool = False,
    ) -> list[FlowInfo]:
        """
        Query for Flows based on standard parameters and return a list of FlowInfo.

        Parameters
        ----------
        job_ids
            One or more strings with uuids of Jobs belonging to the Flow.
        db_ids
            One or more db_ids of Jobs belonging to the Flow.
        flow_ids
            One or more Flow uuids.
        states
            One or more states of the Flow.
        start_date
            Filter Flows updated after this date.
        end_date
            Filter Flows updated before this date.
        name
            Pattern for the Flow name.
        metadata
            A dictionary with metadata to match.
        locked
            If True, query only locked Flows. If False, query any flow (no lock filtering).
        sort
            Sort specification. Accepts:
            - [("field", direction), ...] - explicit direction (1=asc, -1=desc)
            - ["field", ...] - field names only (defaults to ascending)
            - Mixed formats: [("field", -1), "other_field"]
        limit
            Maximum number of entries to retrieve.
        skip
            Number of entries to skip.
        full
            If True, return full FlowInfo objects.

        Returns
        -------
        list[FlowInfo]
            List of FlowInfo objects.
        """
        # Use consolidated flow query building method
        conditions, flow_uuids_from_jobs = self._build_query_flow(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            locked=locked,
        )

        # Early return if no matching flows
        if flow_uuids_from_jobs is not None and len(flow_uuids_from_jobs) == 0:
            return []

        with self.get_session() as session:
            # Build query with job aggregation
            query_stmt = select(SQLFlow)
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            # Sorting - normalize sort parameters to handle both MongoDB formats
            normalized_sort = self._normalize_sort_params(sort)
            if normalized_sort:
                order_clauses = []
                for field, direction in normalized_sort:
                    if hasattr(SQLFlow, field):
                        attr = getattr(SQLFlow, field)
                        if direction == pymongo.DESCENDING or direction == -1:
                            order_clauses.append(attr.desc())
                        else:
                            order_clauses.append(attr.asc())
                if order_clauses:
                    query_stmt = query_stmt.order_by(*order_clauses)

            # Skip and Limit
            if skip > 0:
                query_stmt = query_stmt.offset(skip)
            if limit > 0:
                query_stmt = query_stmt.limit(limit)

            # Execute query
            flows = session.execute(query_stmt).scalars().all()

            # Convert to FlowInfo objects
            # Note: 'full' parameter behavior may need adjustment based on original implementation
            flow_infos = []
            for flow in flows:
                flow_doc = flow.to_flow_doc()
                flow_dict = flow_doc.as_db_dict()

                # Get job information for this flow
                job_uuids = flow_doc.jobs
                jobs_query = select(SQLJob).where(SQLJob.uuid.in_(job_uuids))
                jobs = session.execute(jobs_query).scalars().all()

                # Add jobs_list to flow dict for FlowInfo.from_query_dict
                flow_dict["jobs_list"] = []
                for job in jobs:
                    job_doc = job.to_job_doc()
                    flow_dict["jobs_list"].append(job_doc.as_db_dict())

                flow_info = FlowInfo.from_query_dict(flow_dict)
                flow_infos.append(flow_info)

            return flow_infos

    @staticmethod
    def _normalize_sort_params(
        sort: list[tuple[str, int] | str] | None,
    ) -> list[tuple[str, int]]:
        """
        Normalize sort parameters to list of (field, direction) tuples.
        Accepts both MongoDB-style formats:
        - [("field", direction), ...] - explicit direction
        - ["field", ...] - defaults to ascending (1)
        - Mixed: [("field", -1), "other_field"] - mixed formats

        Parameters
        ----------
        sort
            Sort specification in various formats

        Returns
        -------
        list[tuple[str, int]]
            List of (field, direction) tuples where direction is 1 (asc) or -1 (desc)
        """
        if not sort:
            return []

        normalized = []
        for item in sort:
            if isinstance(item, str):
                # Just field name - default to ascending
                normalized.append((item, 1))
            elif isinstance(item, (tuple, list)) and len(item) == 2:
                # (field, direction) tuple/list
                field, direction = item
                normalized.append((field, direction))
            else:
                raise ValueError(
                    f"Invalid sort specification: {item}. Expected string or (field, direction) tuple."
                )

        return normalized

    @staticmethod
    def generate_job_id_query(
        db_id: str | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
    ) -> tuple[dict, list | None]:
        """
        Generate a query for a single Job based on db_id or uuid+index.
        Only one among db_id and job_id should be defined.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None the Job the sorting will be
            added to get the highest index.

        Returns
        -------
        dict, list
            A dict and an optional list to be used as filter and sort,
            respectively, in a query for a single Job.
        """
        query: dict = {}
        sort: list | None = None

        if (job_id is None) == (db_id is None):
            raise ValueError(
                "One and only one among job_id and db_id should be defined"
            )

        if db_id:
            query["db_id"] = db_id
        if job_id:
            query["uuid"] = job_id
            if job_index is None:
                # Sort by index descending to get highest index
                sort = [("index", pymongo.DESCENDING)]
            else:
                query["index"] = job_index
        if not query:
            raise ValueError("At least one among db_id and job_id should be specified")

        return query, sort

    def _reset_remote(self, doc: dict, delete_files: bool = True) -> dict:
        """
        Simple reset of a Job in a running state or REMOTE_ERROR.
        Does not require additional locking on the Flow or other Jobs.

        Parameters
        ----------
        doc
            The dict of the JobDoc associated to the Job to rerun.
            Just the "uuid", "index", "state" values are required.
        delete_files
            Delete all the files in the worker folder of the rerun Job.

        Returns
        -------
        dict
            Updates to be set on the Job upon lock release.
        """
        if doc["state"] in [JobState.SUBMITTED.value, JobState.RUNNING.value]:
            # try cancelling the job submitted to the remote queue
            try:
                self._cancel_queue_process(doc)
            except Exception:
                logger.warning(
                    f"Failed cancelling the process for Job {doc['uuid']} {doc['index']}",
                    exc_info=True,
                )

        job_doc_update = get_reset_job_base_dict()
        job_doc_update["state"] = JobState.READY.value

        if doc.get("run_dir") and delete_files:
            job_doc_update["remote.prerun_cleanup"] = True

        return job_doc_update

    def _cancel_queue_process(self, doc: dict):
        """
        Cancel a job that was submitted to the remote queue.

        This is a placeholder - actual implementation would depend on
        the specific queue system being used.
        """
        # TODO: Implement actual queue cancellation logic
        # This would typically involve calling the appropriate queue system's
        # cancel method using the job's remote process ID
        logger.info(
            f"Attempting to cancel queue process for job {doc['uuid']} {doc['index']}"
        )

    def _build_query_job(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None,
        db_ids: str | list[str] | None,
        flow_ids: str | list[str] | None,
        states: JobState | list[JobState] | None,
        locked: bool,
        start_date: datetime | None,
        end_date: datetime | None,
        name: str | None,
        metadata: dict | None,
        workers: str | list[str] | None,
        custom_query: dict | None,
    ) -> dict:
        """
        Build a MongoDB-style query to search for Jobs, based on standard parameters.
        The Jobs will need to satisfy all the defined conditions.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids or DB_IDs to which the Jobs to retrieve belong.
        states
            One or more states of the Jobs.
        locked
            If True only locked Jobs will be selected.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        workers
            One or more worker names.
        custom_query
            A generic query. Keys must not overlap with other specified query options.

        Returns
        -------
        dict
            A MongoDB-style dictionary with the query to be applied.
        """
        import fnmatch
        from typing import cast

        from jobflow_remote.utils.data import check_valid_uuid

        if job_ids and not any(isinstance(ji, (list, tuple)) for ji in job_ids):
            # without these cast mypy is confused about the type
            job_ids = cast(list[tuple[str, int]], [job_ids])
        db_ids = [db_ids] if isinstance(db_ids, str) else db_ids or []

        flow_ids = [flow_ids] if isinstance(flow_ids, str) else flow_ids or []
        flow_uuids = []

        # Convert flow_ids to flow_uuids, handling both UUIDs and db_ids
        for fid in flow_ids:
            if check_valid_uuid(fid):
                flow_uuids.append(fid)
            else:
                # Look up flow by db_id in the ids field
                with self.get_session() as session:
                    # Use limit(1).first() to match MongoDB find_one() behavior
                    result = session.execute(
                        select(SQLFlow.uuid)
                        .where(SQLFlow.ids.contains(f'"{fid}"'))
                        .limit(1)
                    ).first()
                    flow_row = result[0] if result else None
                    if flow_row:
                        flow_uuids.append(flow_row)

        if isinstance(states, JobState):
            states = [states]
        if isinstance(workers, str):
            workers = [workers]

        base_query: dict = defaultdict(dict)

        if db_ids:
            base_query["db_id"] = {"$in": db_ids}
        if job_ids:
            job_ids = cast(list[tuple[str, int]], job_ids)
            or_list = []
            for job_id, job_index in job_ids:
                or_list.append({"uuid": job_id, "index": job_index})
            base_query["$or"] = or_list

        if flow_uuids:
            # Direct query on job's hosts field (contains flow UUID) - like MongoDB
            # This eliminates the need for separate flow queries
            base_query["job.hosts"] = {"$in": flow_uuids}

        if states:
            base_query["state"] = {"$in": [s.value for s in states]}

        if start_date:
            start_date_str = start_date.astimezone(timezone.utc)
            base_query["updated_on"] = {"$gte": start_date_str}
        if end_date:
            end_date_str = end_date.astimezone(timezone.utc)
            if "updated_on" not in base_query:
                base_query["updated_on"] = {}
            base_query["updated_on"]["$lte"] = end_date_str

        if locked:
            base_query["lock_id"] = {"$ne": None}

        if name:
            # Add the beginning of the line, so that it will match the string
            # exactly if no wildcard is given. Otherwise will match substrings.
            mongo_regex = "^" + fnmatch.translate(name).replace("\\\\", "\\")
            base_query["job.name"] = {"$regex": mongo_regex}

        if metadata:
            metadata_dict = {f"job.metadata.{k}": v for k, v in metadata.items()}
            base_query.update(metadata_dict)

        if workers:
            base_query["worker"] = {"$in": workers}

        custom_query = custom_query or {}
        if not set(base_query).isdisjoint(custom_query):
            raise ValueError(
                f"Custom_query must not overlap with other query options. Duplicates: {set(base_query) & set(custom_query)}"
            )

        return base_query | custom_query

    def _build_query_flow(
        self,
        job_ids: str | list[str] | None,
        db_ids: str | list[str] | None,
        flow_ids: str | list[str] | None,
        states: FlowState | list[FlowState] | None,
        start_date: datetime | None,
        end_date: datetime | None,
        name: str | None,
        metadata: dict | None,
        locked: bool,
    ) -> tuple[list, set[str] | None]:
        """
        Build query conditions and flow UUID filtering for flow-related queries.

        Parameters
        ----------
        job_ids
            One or more strings with uuids of Jobs belonging to the Flow.
        db_ids
            One or more db_ids of Jobs belonging to the Flow.
        flow_ids
            One or more Flow uuids.
        states
            One or more states of the Flows.
        start_date
            Filter Flows updated after this date.
        end_date
            Filter Flows updated before this date.
        name
            Pattern for the Flow name.
        metadata
            A dictionary with metadata to match.
        locked
            If True, only locked flows will be selected.

        Returns
        -------
        tuple[list, set[str] | None]
            A tuple of (SQL conditions, flow_uuids_from_jobs_filter)
            flow_uuids_from_jobs_filter is None if no job-based filtering was done,
            or a set (possibly empty) if job-based filtering was applied
        """
        conditions = []
        flow_uuids_from_jobs = None  # None means no job-based filtering was done

        # Only open session if we need to query for job_ids or db_ids
        if job_ids is not None or db_ids is not None:
            flow_uuids_from_jobs = set()  # Empty set means job-based filtering was done
            with self.get_session() as session:
                # Job IDs - find flows containing these jobs
                if job_ids is not None:
                    if isinstance(job_ids, str):
                        job_ids = [job_ids]
                    flows_with_jobs = session.execute(
                        select(SQLFlow.uuid, SQLFlow.jobs)
                    ).all()
                    for flow_uuid, jobs_json in flows_with_jobs:
                        jobs_list = jobs_json
                        if any(job_id in jobs_list for job_id in job_ids):
                            flow_uuids_from_jobs.add(flow_uuid)

                # DB IDs - find flows containing jobs with these db_ids
                if db_ids is not None:
                    if isinstance(db_ids, str):
                        db_ids = [db_ids]
                    jobs_with_db_ids = (
                        session.execute(
                            select(SQLJob.uuid).where(SQLJob.db_id.in_(db_ids))
                        )
                        .scalars()
                        .all()
                    )
                    if jobs_with_db_ids:
                        flows_with_jobs = session.execute(
                            select(SQLFlow.uuid, SQLFlow.jobs)
                        ).all()
                        for flow_uuid, jobs_json in flows_with_jobs:
                            jobs_list = jobs_json
                            if any(
                                job_uuid in jobs_list for job_uuid in jobs_with_db_ids
                            ):
                                flow_uuids_from_jobs.add(flow_uuid)

        # Apply job-based filtering if any job/db filtering was specified
        if flow_uuids_from_jobs:
            conditions.append(SQLFlow.uuid.in_(list(flow_uuids_from_jobs)))

        # Flow IDs
        if flow_ids is not None:
            if isinstance(flow_ids, str):
                flow_ids = [flow_ids]
            if flow_uuids_from_jobs:
                # Intersection of flow_ids and flows containing specified jobs
                matching_flows = set(flow_ids) & flow_uuids_from_jobs
                if matching_flows:
                    conditions[-1] = SQLFlow.uuid.in_(list(matching_flows))
                else:
                    # No intersection - add impossible condition
                    conditions.append(SQLFlow.uuid == "__nonexistent__")
            else:
                conditions.append(SQLFlow.uuid.in_(flow_ids))

        # States
        if states is not None:
            if isinstance(states, FlowState):
                states = [states]
            state_values = [s.value for s in states]
            conditions.append(SQLFlow.state.in_(state_values))

        # Date range
        if start_date is not None:
            start_date_str = start_date.astimezone(timezone.utc)
            conditions.append(SQLFlow.updated_on >= start_date_str)
        if end_date is not None:
            end_date_str = end_date.astimezone(timezone.utc)
            conditions.append(SQLFlow.updated_on <= end_date_str)

        # Name pattern
        if name is not None:
            if "*" in name or "?" in name:
                # Use LIKE pattern for wildcards
                like_pattern = name.replace("*", "%").replace("?", "_")
                conditions.append(SQLFlow.name.like(like_pattern))
            else:
                # Exact match
                conditions.append(SQLFlow.name == name)

        # Metadata pattern (search in JSON field)
        if metadata is not None:
            for key, value in metadata.items():
                # Search for key-value pairs in the JSON metadata field
                search_pattern = f'"{key}": "{value}"'
                conditions.append(SQLFlow.flow_metadata.contains(search_pattern))

        # Locked status
        if locked:
            conditions.append(SQLFlow.lock_id.is_not(None))

        return conditions, flow_uuids_from_jobs

    def _many_jobs_action(
        self,
        method: callable,
        action_description: str,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        max_limit: int = 0,
        **method_kwargs,
    ) -> list[str]:
        """
        Helper method to query Jobs based on criteria and sequentially apply an
        action on all those retrieved.

        Used to provide a common interface between all the methods that
        should be applied on a list of jobs sequentially.

        Parameters
        ----------
        method
            The function that should be applied on a single Job.
        action_description
            A description of the action being performed. For logging purposes.
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
        states
            The state of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        workers
            One or more worker names.
        custom_query
            A generic query. Keys must not overlap with other specified query options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        max_limit
            The action will be applied to the Jobs only if the total number is lower
            than the specified limit. 0 means no limit.
        method_kwargs
            Kwargs passed to the method called on each Job

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        query_dict = self._build_query_job(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            locked=False,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
        )

        # Get matching jobs
        with self.get_session() as session:
            # Convert MongoDB-style query to SQL conditions
            conditions = MongoToSQLConverter.convert_query(query_dict, SQLJob)

            # Build query for db_ids only (for performance)
            query_stmt = select(SQLJob.db_id)
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            # Execute query to get db_ids
            result = session.execute(query_stmt).scalars().all()
            queried_db_ids = list(result)

        if max_limit != 0 and len(queried_db_ids) > max_limit:
            raise ValueError(
                f"Cannot perform {action_description} on {len(queried_db_ids)} Jobs "
                f"as they exceeds the specified maximum limit ({max_limit}). "
                f"Increase the limit to complete the action on this many Jobs."
            )

        updated_ids = set()
        for db_id in queried_db_ids:
            try:
                job_updated_ids = method(db_id=db_id, **method_kwargs)
                if not isinstance(job_updated_ids, (list, tuple)):
                    job_updated_ids = (
                        [] if job_updated_ids is None else [job_updated_ids]
                    )
                if job_updated_ids:
                    updated_ids.update(job_updated_ids)
            except Exception:
                if raise_on_error:
                    raise
                logger.exception(f"Error while {action_description} for job {db_id}")

        return list(updated_ids)

    def rerun_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        force: bool = False,
        wait: int | None = None,
        break_lock: bool = False,
        delete_files: bool = True,
    ) -> list[str]:
        """
        Rerun a single Job, i.e. bring its state back to READY.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        By default, only Jobs in one of the running states (CHECKED_OUT,
        UPLOADED, ...), in the REMOTE_ERROR state or FAILED with
        children in the READY or WAITING state can be rerun.
        This should guarantee that no unexpected inconsistencies due to
        dynamic Jobs generation should appear. This limitation can be bypassed
        with the `force` option.
        In any case, no Job with children with index > 1 can be rerun, as there
        is no sensible way of handling it.

        Rerunning a Job in a REMOTE_ERROR or on an intermediate STATE also
        results in a reset of the remote attempts and errors.
        When rerunning a Job in a SUBMITTED or RUNNING state the system also
        tries to cancel the process in the worker.
        Rerunning a FAILED Job also lead to change of state in its children.
        The full list of modified Jobs is returned.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None: the Job with the highest index.
        force
            Bypass the limitation that only Jobs in a certain state can be rerun.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.
        delete_files
            Delete all the files in the worker folder of the rerun Job.
            Note that the deletion will not be performed directly but only when the job effectively restarts.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        lock_filter, sort = self.generate_job_id_query(db_id, job_id, job_index)
        sleep = None
        if wait:
            sleep = 10

        modified_jobs: list[str] = []
        # the job to rerun is the last to be released since this prevents
        # a checkout of the job while the flow is still locked
        with self.lock_job(
            filter=lock_filter,
            break_lock=break_lock,
            sort=sort,
            sleep=sleep,
            max_wait=wait,
            get_locked_doc=True,
        ) as job_lock:
            job_doc_dict = job_lock.locked_document
            if not job_doc_dict:
                if job_lock.unavailable_document:
                    raise JobLockedError.from_job_doc(job_lock.unavailable_document)
                raise ValueError(f"No Job document matching criteria {lock_filter}")
            job_state = JobState(job_doc_dict["state"])

            if job_state in [JobState.READY]:
                raise ValueError("The Job is in the READY state. No need to rerun.")
            if job_state in RESETTABLE_STATES:
                # if in one of the resettable states no need to lock the flow or
                # update children.
                doc_update = self._reset_remote(job_doc_dict, delete_files=delete_files)
                modified_jobs = []
            elif (
                job_state not in [JobState.FAILED, JobState.REMOTE_ERROR] and not force
            ):
                raise ValueError(
                    f"Job in state {job_doc_dict['state']} cannot be rerun. "
                    "Use the 'force' option to override this check."
                )
            else:
                # full restart required
                doc_update, modified_jobs = self._full_rerun(
                    job_doc_dict,
                    sleep=sleep,
                    wait=wait,
                    break_lock=break_lock,
                    force=force,
                    delete_files=delete_files,
                )

            modified_jobs.append(job_doc_dict["db_id"])

            # Set update on release (SQLLock handles field name conversion)
            job_lock.update_on_release = doc_update

        return modified_jobs

    def rerun_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        force: bool = False,
        wait: int | None = None,
        break_lock: bool = False,
        delete_files: bool = True,
    ) -> list[str]:
        """
        Rerun a list of selected Jobs, i.e. bring their state back to READY.
        See the docs of `rerun_job` for more details.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
        states
            One or more states of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        workers
            One or more worker names.
        custom_query
            A generic query. Keys must not overlap with other specified query options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        force
            Bypass the limitation that only failed Jobs can be rerun.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.
        delete_files
            Delete all the files in the worker folder of the Jobs that are rerun.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        return self._many_jobs_action(
            method=self.rerun_job,
            action_description="rerunning",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
            raise_on_error=raise_on_error,
            force=force,
            wait=wait,
            break_lock=break_lock,
            delete_files=delete_files,
        )

    def retry_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> str:
        """
        Retry a single Job, i.e. bring it back to its previous state if REMOTE_ERROR,
        or reset the remote attempts and time of retry if in another running state.
        Jobs in other states cannot be retried.
        The Job is selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        Only locking of the retried Job is required.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None: the Job with the highest index.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        str
            The db_id of the updated Job.
        """

        lock_filter, sort = self.generate_job_id_query(db_id, job_id, job_index)
        sleep = None
        if wait:
            sleep = 10

        with self.lock_job(
            filter=lock_filter,
            sort=sort,
            get_locked_doc=True,
            sleep=sleep,
            max_wait=wait,
            break_lock=break_lock,
        ) as lock:
            doc = lock.locked_document
            if not doc:
                if lock.unavailable_document:
                    raise JobLockedError(
                        f"The Job matching criteria {lock_filter} is locked."
                    )
                raise ValueError(f"No Job matching criteria {lock_filter}")

            state = JobState(doc["state"])
            if state == JobState.REMOTE_ERROR:
                previous_state = doc["previous_state"]
                try:
                    JobState(previous_state)
                except ValueError as exc:
                    raise ValueError(
                        f"The registered previous state: {previous_state} is not a valid state"
                    ) from exc
                set_dict = get_reset_job_base_dict()
                set_dict["state"] = previous_state

                lock.update_on_release = set_dict
            elif state in RUNNING_STATES:
                set_dict = {
                    "remote_step_attempts": 0,
                    "remote_retry_time_limit": None,
                    "remote_error": None,
                    "remote_queue_out": None,
                    "remote_queue_err": None,
                }
                lock.update_on_release = set_dict
            else:
                raise ValueError(f"Job in state {state.value} cannot be retried.")
            return doc["db_id"]

    def retry_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[str]:
        """
        Retry selected Jobs, i.e. bring them back to its previous state if REMOTE_ERROR,
        or reset the remote attempts and time of retry if in another running state.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
        states
            One or more states of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        workers
            One or more worker names.
        custom_query
            A generic query. Keys must not overlap with other specified query options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        return self._many_jobs_action(
            method=self.retry_job,
            action_description="retrying",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
            raise_on_error=raise_on_error,
            wait=wait,
            break_lock=break_lock,
        )

    def pause_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
    ) -> str:
        """
        Pause a single Job. Only READY and WAITING Jobs can be paused.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.
        The action is reversible.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None: the Job with the highest index.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.

        Returns
        -------
        str
            The db_id of the updated Job.
        """
        job_lock_kwargs = dict()  # Projection ignored in SQL implementation
        flow_lock_kwargs = dict()  # Projection ignored in SQL implementation
        with self.lock_job_flow(
            acceptable_states=PAUSABLE_STATES,
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            wait=wait,
            break_lock=False,
            job_lock_kwargs=job_lock_kwargs,
            flow_lock_kwargs=flow_lock_kwargs,
        ) as (job_lock, flow_lock):
            job_doc = job_lock.locked_document
            if job_doc is None:
                raise RuntimeError("No job document found in lock")
            job_uuid = job_doc["uuid"]
            job_idx = job_doc["index"]
            updated_states = {job_uuid: {job_idx: JobState.PAUSED}}
            flow_doc = flow_lock.locked_document
            if flow_doc is None:
                raise RuntimeError("No flow document found in lock")
            self.update_flow_state(
                flow_uuid=flow_doc["uuid"],
                updated_states=updated_states,
            )
            job_lock.update_on_release = {"state": JobState.PAUSED.value}
            return_doc = job_lock.locked_document
            if return_doc is None:
                raise RuntimeError("No document found in final job lock")
            return return_doc["db_id"]

    def pause_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
    ) -> list[str]:
        """
        Pause selected Jobs. Only READY and WAITING Jobs can be paused.
        The action is reversible.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
        states
            One or more states of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        workers
            One or more worker names.
        custom_query
            A generic query. Keys must not overlap with other specified query options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        return self._many_jobs_action(
            method=self.pause_job,
            action_description="pausing",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
            raise_on_error=raise_on_error,
            wait=wait,
        )

    def resume_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> str:
        """
        Restart a single Job that was previously paused or stopped.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None: the Job with the highest index.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        str
            The db_id of the updated Job.
        """
        job_lock_kwargs = dict()  # Projection ignored in SQL implementation
        flow_lock_kwargs = dict()  # Projection ignored in SQL implementation
        with self.lock_job_flow(
            acceptable_states=[
                JobState.PAUSED,
                JobState.STOPPED,
                JobState.USER_STOPPED,
            ],
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            wait=wait,
            break_lock=break_lock,
            job_lock_kwargs=job_lock_kwargs,
            flow_lock_kwargs=flow_lock_kwargs,
        ) as (job_lock, flow_lock):
            job_doc = job_lock.locked_document
            if job_doc is None:
                raise RuntimeError("No job document found in lock")
            job_uuid = job_doc["uuid"]
            job_idx = job_doc["index"]
            final_state = self._resume_job_locked(job_doc)

            updated_states = {job_uuid: {job_idx: final_state}}
            self.update_flow_state(
                flow_uuid=flow_lock.locked_document["uuid"],
                updated_states=updated_states,
            )
            job_lock.update_on_release = {"state": final_state.value}
            return job_lock.locked_document["db_id"]

    def _resume_job_locked(self, job_doc: dict) -> JobState:
        """
        Helper method for the logic of resuming a Job.
        Assumes the input is the dictionary representation of a JobDoc and that
        the Flow and Job has been locked.

        Parameters
        ----------
        job_doc
            Dictionary representing the JobDoc with the required elements present.

        Returns
        -------
        JobState
            The final state that should be set to a Job.
        """
        on_missing = job_doc["job"]["config"]["on_missing_references"]
        allow_failed = on_missing != OnMissing.ERROR.value

        # Check parent job states - in principle the lock on each of the parent jobs
        # is not needed since a parent Job cannot change to COMPLETED or FAILED while
        # the flow is locked
        with self.get_session() as session:
            if job_doc["parents"]:
                for parent_job in session.execute(
                    select(SQLJob).where(SQLJob.uuid.in_(job_doc["parents"]))
                ).scalars():
                    parent_state = JobState(parent_job.state)
                    if parent_state != JobState.COMPLETED:
                        if parent_state == JobState.FAILED and allow_failed:
                            continue
                        final_state = JobState.WAITING
                        break
                else:
                    final_state = JobState.READY
            else:
                # No parents, can be set to READY
                final_state = JobState.READY

        return final_state

    def resume_flow(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        flow_id: str | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> int:
        """
        Resume a Flow by resuming all the STOPPED, USER_STOPPED and PAUSED Jobs
        in the Flow.

        Parameters
        ----------
        job_id
            The uuid of one of the Jobs of the Flow.
        db_id
            The db_id of one of the Jobs of the Flow.
        flow_id
            The uuid of the Flow.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        int
            The number of Jobs modified.
        """
        sleep = None
        if wait:
            sleep = 10
        flow_filter = self.generate_flow_id_query(
            job_id=job_id, db_id=db_id, flow_id=flow_id
        )
        with self.lock_flow(
            filter=flow_filter,
            sleep=sleep,
            max_wait=wait,
            get_locked_doc=True,
            break_lock=break_lock,
        ) as flow_lock:
            if not flow_lock.locked_document:
                if flow_lock.unavailable_document:
                    raise FlowLockedError.from_flow_doc(flow_lock.unavailable_document)
                raise ValueError(f"No Flow document matching criteria {flow_filter}")
            flow_doc = FlowDoc.model_validate(flow_lock.locked_document)
            if flow_doc.state not in [FlowState.STOPPED, FlowState.PAUSED]:
                raise ValueError(f"Cannot resume a Flow in state {flow_doc.state}")

            # Find all STOPPED, USER_STOPPED and PAUSED jobs in the flow
            with self.get_session() as session:
                jobs_to_resume = session.execute(
                    select(
                        SQLJob.db_id,
                        SQLJob.uuid,
                        SQLJob.index,
                        SQLJob.state,
                        SQLJob.parents,
                        SQLJob.job,
                    ).where(
                        and_(
                            SQLJob.uuid.in_(flow_doc.jobs),
                            SQLJob.state.in_(
                                [
                                    JobState.STOPPED.value,
                                    JobState.USER_STOPPED.value,
                                    JobState.PAUSED.value,
                                ]
                            ),
                        )
                    )
                ).all()

            job_lock_kwargs = dict()  # Projection ignored in SQL implementation
            n_updated_jobs = 0
            for job_row in jobs_to_resume:
                job_lock_filter = {"db_id": job_row.db_id}
                with self.lock_job(
                    filter=job_lock_filter,
                    break_lock=break_lock,
                    sleep=sleep,
                    max_wait=wait,
                    get_locked_doc=True,
                    **job_lock_kwargs,
                ) as job_lock:
                    job_doc_dict = job_lock.locked_document
                    if not job_doc_dict:
                        if job_lock.unavailable_document:
                            raise JobLockedError.from_job_doc(
                                job_lock.unavailable_document
                            )
                        raise ValueError(
                            f"No Job document matching criteria {job_lock_filter}"
                        )
                    # this should not happen, but handle the case to avoid inconsistencies
                    # no error is raised as the job should already be in the correct state
                    if JobState(job_doc_dict["state"]) not in [
                        JobState.STOPPED,
                        JobState.USER_STOPPED,
                        JobState.PAUSED,
                    ]:
                        continue
                    final_state = self._resume_job_locked(job_doc_dict)
                    job_lock.update_on_release = {"state": final_state.value}
                    n_updated_jobs += 1

            # no need for updated states, since all the Jobs have been already updated separately
            final_state = self.update_flow_state(flow_uuid=flow_doc.uuid)
            if final_state in [FlowState.PAUSED, FlowState.STOPPED]:
                logger.warning(
                    "The Flow was not fully resumed. Consider running resume again"
                )

            return n_updated_jobs

    def resume_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[str]:
        """
        Restart selected Jobs that were previously paused or stopped.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
        states
            One or more states of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        workers
            One or more worker names.
        custom_query
            A generic query. Keys must not overlap with other specified query options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        return self._many_jobs_action(
            method=self.resume_job,
            action_description="resuming",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
            raise_on_error=raise_on_error,
            wait=wait,
            break_lock=break_lock,
        )

    def stop_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> str:
        """
        Stop a single Job. Only Jobs in the READY and all the running states
        can be stopped.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.
        The action is not reversible.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None: the Job with the highest index.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        str
            The db_id of the updated Job.
        """
        job_lock_kwargs = dict()  # Projection ignored in SQL implementation
        flow_lock_kwargs = dict()  # Projection ignored in SQL implementation
        with self.lock_job_flow(
            acceptable_states=[JobState.READY, *RUNNING_STATES],
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            wait=wait,
            break_lock=break_lock,
            job_lock_kwargs=job_lock_kwargs,
            flow_lock_kwargs=flow_lock_kwargs,
        ) as (job_lock, flow_lock):
            job_doc = job_lock.locked_document
            if job_doc is None:
                raise RuntimeError("No job document found in lock")

            job_state = JobState(job_doc["state"])
            if job_state in [JobState.SUBMITTED, JobState.RUNNING]:
                # try cancelling the job submitted to the remote queue
                try:
                    self._cancel_queue_process(job_doc)
                except Exception:
                    logger.warning(
                        f"Failed cancelling the process for Job {job_doc['uuid']} {job_doc['index']}",
                        exc_info=True,
                    )
            job_uuid = job_doc["uuid"]
            job_idx = job_doc["index"]
            updated_states = {job_uuid: {job_idx: JobState.USER_STOPPED}}
            if flow_lock.locked_document is None:
                raise RuntimeError("No document found in flow lock")
            self.update_flow_state(
                flow_uuid=flow_lock.locked_document["uuid"],
                updated_states=updated_states,
            )
            job_lock.update_on_release = {"state": JobState.USER_STOPPED.value}
            return_doc = job_lock.locked_document
            if return_doc is None:
                raise RuntimeError("No document found in final job lock")

            return return_doc["db_id"]

    def stop_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> list[str]:
        """
        Stop selected Jobs. Only Jobs in the READY and all the running states
        can be stopped.
        The action is not reversible.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
        states
            One or more states of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        workers
            One or more worker names.
        custom_query
            A generic query. Keys must not overlap with other specified query options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents. Use with care and
            verify that the lock has been set by a process that is not running
            anymore. Doing otherwise will likely lead to inconsistencies in the DB.

        Returns
        -------
        list
            List of db_ids of the updated Jobs.
        """
        return self._many_jobs_action(
            method=self.stop_job,
            action_description="stopping",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
            raise_on_error=raise_on_error,
            wait=wait,
            break_lock=break_lock,
        )

    def _cancel_queue_process(self, job_doc: dict) -> None:
        """
        Cancel the process in the remote queue.

        Parameters
        ----------
        job_doc
            The dict of the JobDoc with the Job to be cancelled.
        """
        queue_process_id = job_doc["remote"]["process_id"]
        if not queue_process_id:
            raise ValueError("The process id is not defined in the job document")
        with SharedHosts(self.project) as shared_hosts:
            worker = self.project.workers[job_doc["worker"]]
            host = shared_hosts.get_host(job_doc["worker"])

            from qtoolkit.core.data_objects import CancelStatus

            from jobflow_remote.remote.queue import QueueManager

            queue_manager = QueueManager(worker.get_scheduler_io(), host)
            cancel_result = queue_manager.cancel(queue_process_id)
            if cancel_result.status != CancelStatus.SUCCESSFUL:
                raise RuntimeError(
                    f"Cancelling queue process {queue_process_id} failed. "
                    f"Status: {cancel_result.status}, Message: {cancel_result.message}"
                )

    def delete_job(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        delete_output: bool = False,
        delete_files: bool = False,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> str:
        """
        Delete a single job from the queue store and optionally from the job store.
        The Flow document will be updated accordingly but no consistency check
        is performed. The Flow may be left in an inconsistent state.
        For advanced users only.

        Parameters
        ----------
        job_id
            The uuid of the job to delete.
        db_id
            The db_id of the job to delete.
        job_index
            The index of the job. If None, the job with the largest index will be selected.
        delete_output : bool, default False
            If True, also delete the job output from the JobStore.
        delete_files
            If True also delete the files on the worker.
        wait
            In case the Flow or Job is locked, wait this time (in seconds) for the lock to be released.
        break_lock
            Forcibly break the lock on locked documents.

        Returns
        -------
        str
            The db_id of the deleted Job.
        """
        job_lock_kwargs = dict()  # Projection ignored in SQL implementation
        # avoid deleting jobs in batch states. It would require additional
        # specific handling and it is an unlikely use case.
        with self.lock_job_flow(
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            acceptable_states=DELETABLE_STATES,
            wait=wait,
            break_lock=break_lock,
            job_lock_kwargs=job_lock_kwargs,
        ) as (job_lock, flow_lock):
            job_doc = job_lock.locked_document
            if job_doc is None:
                raise RuntimeError("No job document found in lock")
            if flow_lock.locked_document is None:
                raise RuntimeError("No document found in flow lock")

            # Update FlowDoc
            flow_doc = FlowDoc.model_validate(flow_lock.locked_document)
            job_uuid, job_idx = job_doc["uuid"], job_doc["index"]

            if len(flow_doc.ids) == 1:
                raise RuntimeError(
                    "It is not possible to delete the only Job of the Flow. Delete the entire Flow."
                )

            # Remove job from ids list
            flow_doc.ids = [
                id_tuple
                for id_tuple in flow_doc.ids
                if id_tuple[1] != job_uuid or id_tuple[2] != job_idx
            ]

            # Remove job from jobs list and as parent of other jobs if no job
            # with that id remains in the flow
            if not any(job_uuid == id_tuple[1] for id_tuple in flow_doc.ids):
                # Here a flow_doc.jobs.remove could be enough. But due to a previous
                # bug the list of jobs could contain the same uuid more than once.
                # Make sure to remove all the instances.
                flow_doc.jobs = [jid for jid in flow_doc.jobs if jid != job_uuid]

                for parent_dict in flow_doc.parents.values():
                    for index_list in parent_dict.values():
                        if job_uuid in index_list:
                            index_list.remove(job_uuid)

            # Remove job from parents
            flow_doc.parents[job_uuid].pop(str(job_idx), None)
            # if all the jobs with a given uuid have been removed, also remove
            # the entry from the parents
            if not flow_doc.parents[job_uuid]:
                flow_doc.parents.pop(job_uuid, None)

            # Update flow state if necessary
            updated_states: dict[str, dict[int, Any]] = {
                job_uuid: {job_idx: None}
            }  # None indicates job removal
            self.update_flow_state(
                flow_uuid=flow_doc.uuid, updated_states=updated_states
            )

            # Prepare flow update
            flow_update = {
                "jobs": flow_doc.jobs,
                "ids": flow_doc.ids,
                "parents": flow_doc.parents,
                "updated_on": datetime.utcnow(),
            }

            # Set flow update to be applied on lock release
            flow_lock.update_on_release = flow_update

            job_lock.delete_on_release = True

            # Optionally delete from jobstore
            if delete_output:
                jobstore = self.jobstore
                if self.optional_jobstores:
                    store_name = self.get_flow_store(job_doc["job"]["hosts"][-1])
                    if store_name:
                        jobstore = self.optional_jobstores[store_name]
                try:
                    jobstore.remove_docs({"uuid": job_uuid, "index": job_idx})
                except Exception:
                    warnings.warn(
                        f"Error while delete the output of job {job_uuid} {job_idx}",
                        stacklevel=2,
                    )

            if delete_files:
                job_info = JobInfo.from_query_output(job_doc)
                self._safe_delete_files([job_info])

            return job_doc["db_id"]

    def delete_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
        wait: int | None = None,
        delete_output: bool = False,
        delete_files: bool = False,
        max_limit: int = 10,
    ) -> list[str]:
        """
        Delete selected jobs from the queue store and optionally from the job store.
        The Flow document will be updated accordingly but no consistency check
        is performed. The Flow may be left in an inconsistent state.
        For advanced users only.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
        states
            One or more states of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        workers
            One or more worker names.
        custom_query
            A generic query. Keys must not overlap with other specified query options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.
        wait
            In case the Flow or Jobs that need to be deleted are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        delete_output : bool, default False
            If True, also delete the Job output from the JobStore.
        delete_files
            If True also delete the files on the worker.
        max_limit
            The Jobs will be deleted only if the total number is lower than the
            specified limit. 0 means no limit.

        Returns
        -------
        list
            List of db_ids of the deleted Jobs.
        """
        # Open the SharedHosts so that hosts will be shared for all the Jobs
        with SharedHosts(self.project):
            return self._many_jobs_action(
                method=self.delete_job,
                action_description="deleting",
                job_ids=job_ids,
                db_ids=db_ids,
                flow_ids=flow_ids,
                states=states,
                start_date=start_date,
                end_date=end_date,
                name=name,
                metadata=metadata,
                workers=workers,
                custom_query=custom_query,
                raise_on_error=raise_on_error,
                wait=wait,
                delete_output=delete_output,
                delete_files=delete_files,
                max_limit=max_limit,
            )

    def _full_rerun(
        self,
        doc: dict,
        sleep: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
        force: bool = False,
        delete_files: bool = True,
    ) -> tuple[dict, list[str]]:
        """
        Perform the full rerun of Job, in case a Job is FAILED or in one of the
        usually not admissible states. This requires actions on the original
        Job's children and will need to acquire the lock on all of them as well
        as on the Flow.

        Parameters
        ----------
        doc
            The dict of the JobDoc associated to the Job to rerun.
        sleep
            Sleep time for lock acquisition.
        wait
            Maximum wait time for locks.
        break_lock
            Whether to break existing locks.
        force
            Whether to force the rerun.
        delete_files
            Whether to delete files.

        Returns
        -------
        tuple[dict, list[str]]
            Updates for the job and list of modified job db_ids.
        """
        # TODO: Implement full rerun logic
        # This is a complex method that needs to:
        # 1. Lock the flow containing this job
        # 2. Find and lock all children jobs
        # 3. Reset job states appropriately
        # 4. Handle dynamic job dependencies

        from contextlib import ExitStack

        from jobflow_remote.jobs.data import FlowDoc, get_reset_job_base_dict

        job_id = doc["uuid"]
        job_index = doc["index"]
        modified_jobs: list[str] = []

        flow_filter = {"jobs": job_id}
        with self.lock_flow(
            filter=flow_filter,
            sleep=sleep,
            max_wait=wait,
            get_locked_doc=True,
            break_lock=break_lock,
        ) as flow_lock:
            if not flow_lock.locked_document:
                if flow_lock.unavailable_document:
                    raise FlowLockedError.from_flow_doc(flow_lock.unavailable_document)
                raise ValueError(f"No Flow document matching criteria {flow_filter}")

            flow_doc = FlowDoc.model_validate(flow_lock.locked_document)

            # only the job with the largest index currently present in the db
            # can be rerun to avoid inconsistencies. (rerunning a smaller index
            # would still leave the job with larger indexes in the DB with no
            # clear way of how to deal with them)
            if max(flow_doc.ids_mapping[job_id]) > job_index:
                raise ValueError(
                    f"Job {job_id} is not the highest index ({job_index}). "
                    "Rerunning it will lead to inconsistencies and is not allowed."
                )

            # check that the all the children only those with the largest index
            # in the flow are present.
            # If that is the case the rerun would lead to inconsistencies.
            # If only the last one is among the children it is acceptable
            # to rerun, but in case of a child with lower index a dynamical
            # action that cannot be reverted has been already applied.
            # Do not allow this even if force==True.
            # if not force, only the first level children need to be checked
            if not force:
                descendants = flow_doc.children.get(job_id, [])
            else:
                descendants = flow_doc.descendants(job_id)
            for dep_id, dep_index in descendants:
                if max(flow_doc.ids_mapping[dep_id]) > dep_index:
                    raise ValueError(
                        f"Job {job_id} has a child job ({dep_id}) which is not the last index ({dep_index}). "
                        "Rerunning the Job will lead to inconsistencies and is not allowed."
                    )

            # TODO should STOPPED be acceptable?
            acceptable_child_states = [
                JobState.READY.value,
                JobState.WAITING.value,
                JobState.PAUSED.value,
            ]
            # Update the state of the descendants
            updated_states: dict[str, dict[int, JobState]] = defaultdict(dict)
            with ExitStack() as stack:
                # first acquire the lock on all the descendants and
                # check their state if needed. Break immediately if
                # the lock cannot be acquired on one of the children
                # or if the states do not satisfy the requirements
                children_locks = []
                for dep_id, dep_index in descendants:
                    # TODO consider using the db_id for the query. may be faster?
                    child_lock = stack.enter_context(
                        self.lock_job(
                            filter={"uuid": dep_id, "index": dep_index},
                            break_lock=break_lock,
                            sleep=sleep,
                            max_wait=wait,
                            get_locked_doc=True,
                        )
                    )
                    child_doc_dict = child_lock.locked_document
                    if not child_doc_dict:
                        if child_lock.unavailable_document:
                            raise JobLockedError.from_job_doc(
                                child_lock.unavailable_document,
                                f"The parent Job with uuid {job_id} cannot be rerun",
                            )
                        raise ValueError(
                            f"The child of Job {job_id} to rerun with uuid {dep_id} and index {dep_index} could not be found in the database"
                        )

                    # check that the children have not been started yet.
                    # the only case being if some children allow failed parents.
                    # Put a lock on each of the children, so that if they are READY
                    # they will not be checked out
                    if (
                        not force
                        and child_doc_dict["state"] not in acceptable_child_states
                    ):
                        msg = (
                            f"The child of Job {job_id} to rerun with uuid {dep_id} and "
                            f"index {dep_index} has state {child_doc_dict['state']} which "
                            "is not acceptable. Use the 'force' option to override this check."
                        )
                        raise ValueError(msg)
                    children_locks.append(child_lock)

                # Here all the descendants are locked and could be set to WAITING.
                # Set the new state for all of them.
                for child_lock in children_locks:
                    child_doc = child_lock.locked_document
                    child_doc_update = get_reset_job_base_dict()
                    child_doc_update["state"] = JobState.WAITING.value
                    if child_doc["state"] != JobState.WAITING.value:
                        modified_jobs.append(child_doc["db_id"])
                        if delete_files:
                            child_doc_update["remote.prerun_cleanup"] = True
                    child_lock.update_on_release = {"$set": child_doc_update}
                    updated_states[child_doc["uuid"]][child_doc["index"]] = (
                        JobState.WAITING
                    )
                    self._delete_tmp_folder(child_doc)

            # if everything is fine here, update the state of the flow
            # before releasing its lock and set the update for the original job
            # pass explicitly the new state of the job, since it is not updated
            # in the DB. The Job is the last lock to be released.
            updated_states[job_id][job_index] = JobState.READY
            self.update_flow_state(
                flow_uuid=flow_doc.uuid, updated_states=updated_states
            )

            # delete local temporary folder to avoid parsing
            # previously downloaded files.
            self._delete_tmp_folder(doc)
            job_doc_update = get_reset_job_base_dict()
            job_doc_update["state"] = JobState.READY.value
            if delete_files:
                job_doc_update["remote.prerun_cleanup"] = True

        return job_doc_update, modified_jobs

    @contextlib.contextmanager
    def lock_job_flow(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
        acceptable_states: list[JobState] | None = None,
        job_lock_kwargs: dict | None = None,
        flow_lock_kwargs: dict | None = None,
    ) -> Generator[tuple[SQLLock, SQLLock], None, None]:
        """
        Lock one Job document and the Flow document the Job belongs to.

        See SQLLock context manager for more details about the locking options.

        Parameters
        ----------
        job_id
            The uuid of the Job to lock.
        db_id
            The db_id of the Job to lock.
        job_index
            The index of the Job to lock.
        wait
            The amount of seconds to wait for a lock to be released.
        break_lock
            True if the context manager is allowed to forcibly break a lock.
        acceptable_states
            A list of JobStates. If not among these a ValueError exception is
            raised.
        job_lock_kwargs
            Kwargs passed to SQLLock for the Job lock.
        flow_lock_kwargs
            Kwargs passed to SQLLock for the Flow lock.

        Returns
        -------
        SQLLock, SQLLock
            Tuple of (job_lock, flow_lock) instances.
        """
        lock_filter, sort = self.generate_job_id_query(db_id, job_id, job_index)
        sleep = None
        if wait:
            sleep = 10
        job_lock_kwargs = job_lock_kwargs or {}
        flow_lock_kwargs = flow_lock_kwargs or {}

        with self.lock_job(
            filter=lock_filter,
            break_lock=break_lock,
            sort=sort,
            sleep=sleep,
            max_wait=wait,
            get_locked_doc=True,
            **job_lock_kwargs,
        ) as job_lock:
            job_doc_dict = job_lock.locked_document
            if not job_doc_dict:
                if job_lock.unavailable_document:
                    raise JobLockedError.from_job_doc(job_lock.unavailable_document)
                raise ValueError(f"No Job document matching criteria {lock_filter}")
            job_state = JobState(job_doc_dict["state"])
            if acceptable_states and job_state not in acceptable_states:
                raise ValueError(
                    f"Job in state {job_doc_dict['state']}. The action cannot be performed"
                )

            flow_filter = {"jobs": job_doc_dict["uuid"]}
            with self.lock_flow(
                filter=flow_filter,
                sleep=sleep,
                max_wait=wait,
                get_locked_doc=True,
                break_lock=break_lock,
                **flow_lock_kwargs,
            ) as flow_lock:
                if not flow_lock.locked_document:
                    if flow_lock.unavailable_document:
                        raise FlowLockedError.from_flow_doc(
                            flow_lock.unavailable_document
                        )
                    raise ValueError(
                        f"No Flow document matching criteria {flow_filter}"
                    )

                yield job_lock, flow_lock

    @contextlib.contextmanager
    def lock_flow(self, **lock_kwargs) -> Generator[SQLLock, None, None]:
        """
        Lock a Flow document.

        See SQLLock context manager for more details about the locking options.

        Parameters
        ----------
        lock_kwargs
            Kwargs passed to the SQLLock context manager.

        Returns
        -------
        SQLLock
            An instance of SQLLock.
        """
        with self.get_session() as session:
            # Extract filter from lock_kwargs
            filter_dict = lock_kwargs.pop("filter", {})

            # Convert MongoDB query for flows containing jobs
            if "jobs" in filter_dict:
                job_id = filter_dict["jobs"]
                # Find flow containing this job
                # Use limit(1).first() to match MongoDB find_one() behavior
                result = session.execute(
                    select(SQLFlow).where(SQLFlow.jobs.contains(f'"{job_id}"')).limit(1)
                ).first()
                flow_row = result[0] if result else None

                if flow_row:
                    # Use the specific flow UUID for locking
                    lock_filter = {"uuid": flow_row.uuid}
                else:
                    # No flow found containing this job
                    lock_filter = {"uuid": "__nonexistent__"}
            else:
                lock_filter = filter_dict

            lock = SQLLock(
                session=session, table_class=SQLFlow, filter=lock_filter, **lock_kwargs
            )

            with lock:
                yield lock

    def _delete_tmp_folder(self, doc: dict):
        """
        Delete temporary folder for a job.

        This method attempts to remove the local temporary directory
        associated with a job if it exists.

        Parameters
        ----------
        doc
            Job document dictionary containing job information.
        """
        import shutil
        import tempfile
        from pathlib import Path

        try:
            # Construct temp folder path from job info
            # The temp folder is typically based on job db_id or uuid
            job_uuid = doc.get("uuid")
            job_index = doc.get("index", 0)
            db_id = doc.get("db_id")

            if not job_uuid:
                logger.warning(
                    "Cannot delete temp folder: job uuid not found in document"
                )
                return

            # Try multiple possible temp folder patterns
            # Pattern 1: Based on db_id if available
            temp_paths = []
            if db_id:
                temp_paths.extend(
                    [
                        Path(tempfile.gettempdir()) / f"jfremote_{db_id}",
                        Path(tempfile.gettempdir()) / "jfremote" / f"{db_id}",
                    ]
                )

            # Pattern 2: Based on uuid and index
            temp_paths.extend(
                [
                    Path(tempfile.gettempdir()) / f"jfremote_{job_uuid}_{job_index}",
                    Path(tempfile.gettempdir())
                    / "jfremote"
                    / f"{job_uuid}_{job_index}",
                    Path(tempfile.gettempdir()) / f"jfremote_{job_uuid}",
                    Path(tempfile.gettempdir()) / "jfremote" / f"{job_uuid}",
                ]
            )

            # Try to delete any existing temp folders
            deleted_any = False
            for temp_path in temp_paths:
                if temp_path.exists() and temp_path.is_dir():
                    try:
                        shutil.rmtree(temp_path)
                        logger.debug(f"Deleted temp folder: {temp_path}")
                        deleted_any = True
                    except OSError as e:
                        logger.warning(f"Failed to delete temp folder {temp_path}: {e}")

            if not deleted_any:
                logger.debug(
                    f"No temp folder found to delete for job {job_uuid} {job_index}"
                )

        except Exception as e:
            logger.warning(
                f"Error while attempting to delete temp folder for job {doc.get('uuid')} {doc.get('index')}: {e}"
            )
            # Don't raise - temp folder deletion is not critical for job operation

    def get_flow_info_by_flow_uuid(self, flow_uuid: str) -> dict | None:
        """Get FlowDoc dictionary by flow UUID."""
        with self.get_session() as session:
            flow_row = session.execute(
                select(SQLFlow).where(SQLFlow.uuid == flow_uuid)
            ).scalar_one_or_none()

            if flow_row:
                flow_doc = flow_row.to_flow_doc()
                return flow_doc.as_db_dict()

            return None

    def get_flow_info_by_job_uuid(self, job_uuid: str) -> dict | None:
        """Get FlowDoc dictionary by job UUID."""
        with self.get_session() as session:
            # Find flow containing this job
            # Use limit(1).first() to match MongoDB find_one() behavior
            result = session.execute(
                select(SQLFlow).where(SQLFlow.jobs.contains(f'"{job_uuid}"')).limit(1)
            ).first()
            flow_row = result[0] if result else None

            if flow_row:
                flow_doc = flow_row.to_flow_doc()
                return flow_doc.as_db_dict()

            return None

    def get_job_info_by_job_uuid(
        self,
        job_uuid: str,
        job_index: int | str = "last",
        projection: list | dict | None = None,
    ) -> dict | None:
        """
        Get JobDoc dictionary by job UUID and optional index.

        Parameters
        ----------
        job_uuid
            The UUID of the job.
        job_index
            The index of the Job. Can be "last" (default) to get the highest index,
            or a specific integer index.
        projection
            Ignored in SQL implementation. Kept for API compatibility with MongoDB version.
            In MongoDB, projection reduces data transfer from DB. In SQL, we fetch the
            entire row anyway, so applying projection in Python adds overhead without benefit.

        Returns
        -------
        dict | None
            The JobDoc dictionary or None if not found.
        """
        # Note: projection parameter is ignored - see docstring for explanation
        with self.get_session() as session:
            conditions = [SQLJob.uuid == job_uuid]
            sort = None

            if isinstance(job_index, int):
                conditions.append(SQLJob.index == job_index)
            elif job_index == "last":
                sort = [("index", pymongo.DESCENDING)]
            else:
                raise ValueError(f"job_index value: {job_index} is not supported")

            query_stmt = select(SQLJob).where(and_(*conditions))

            # Apply sorting if needed
            if sort:
                for field, direction in sort:
                    if hasattr(SQLJob, field):
                        attr = getattr(SQLJob, field)
                        if direction == pymongo.DESCENDING:
                            query_stmt = query_stmt.order_by(attr.desc())
                        else:
                            query_stmt = query_stmt.order_by(attr.asc())

            # Use limit(1) + first() to get the first result after sorting (like MongoDB find_one())
            result = session.execute(query_stmt.limit(1)).first()
            job = result[0] if result else None

            if job:
                job_doc = job.to_job_doc()
                return job_doc.as_db_dict()

            return None

    def get_flow_store(self, flow_id: str) -> str | None:
        """
        Fetch the name of the optional JobStore to store the outputs
        of a Flow, if defined.

        Parameters
        ----------
        flow_id
            The uuid of the Flow
        Returns
        -------
        str
            The name of one of the optional JobStores to be used.
            None if the default should be used.
        """
        with self.get_session() as session:
            # Query for the flow with the given uuid
            flow_row = session.execute(
                select(SQLFlow).where(SQLFlow.uuid == flow_id)
            ).scalar_one_or_none()

            if flow_row is None:
                raise ValueError(f"No Flow matching id {flow_id}")

            return flow_row.jobstore or None

    @staticmethod
    def generate_flow_id_query(
        db_id: str | None = None,
        job_id: str | None = None,
        flow_id: str | None = None,
    ) -> dict:
        """
        Generate a query for a single Flow based on the ids.
        Only one among the input options should be defined.

        Parameters
        ----------
        db_id
            The db_id of one Job belonging to the Flow.
        job_id
            The uuid of one Job belonging to the Flow.
        flow_id
            The uuid of the Flow.

        Returns
        -------
        dict
            A dict to be used as filter in a query for a single Flow.
        """

        if sum((job_id is None, db_id is None, flow_id is None)) != 2:
            raise ValueError(
                "One and only one among job_id, db_id and flow_id should be defined"
            )

        if db_id:
            # Search for flows where the ids field contains this db_id
            return {"ids": {"$elemMatch": {"0": db_id}}}
        if job_id:
            # Search for flows where the jobs field contains this job_id
            return {"jobs": job_id}

        return {"uuid": flow_id}

    def set_flow_store(
        self,
        store: str | None,
        db_id: str | None = None,
        job_id: str | None = None,
        flow_id: str | None = None,
    ) -> None:
        """
        Set the name of the optional JobStore to store the outputs
        of a Flow. If None the default JobStore will be used.
        Can be changed only for READY Flows.

        Parameters
        ----------
        store
            The name of one of the optional JobStores. If None sets to the
            default JobStore.
        db_id
            The db_id of one Job belonging to the Flow.
        job_id
            The uuid of one Job belonging to the Flow.
        flow_id
            The uuid of the Flow.

        """
        if store and store not in self.optional_jobstores:
            raise ValueError(f"Store {store} is not defined as an optional jobstore")

        filter_query = self.generate_flow_id_query(
            db_id=db_id, job_id=job_id, flow_id=flow_id
        )
        with self.lock_flow(filter=filter_query, get_locked_doc=True) as flow_lock:
            if not flow_lock.locked_document:
                if flow_lock.unavailable_document:
                    raise FlowLockedError.from_flow_doc(flow_lock.unavailable_document)
                raise ValueError(f"No Flow document matching criteria {filter_query}")
            if FlowState(flow_lock.locked_document["state"]) != FlowState.READY:
                raise RuntimeError("The JobStore can be set only for a READY Flow")
            flow_lock.update_on_release = {
                "jobstore": store,
            }

    def set_job_doc_properties(
        self,
        values: dict,
        db_id: str | None = None,
        job_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
        acceptable_states: list[JobState] | None = None,
        use_pipeline: bool = False,
    ) -> str:
        """
        Helper to set multiple values in a JobDoc while locking the Job.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        Parameters
        ----------
        values
            Dictionary with the values to be set. Will be passed to a pymongo
            `update_one` method.
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None the Job with the largest index
            will be selected.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents.
        acceptable_states
            List of JobState for which the Job values can be changed.
            If None all states are acceptable.
        use_pipeline
            if True a pipeline will be used in the update of the document

        Returns
        -------
        str
            The db_id of the updated Job. None if the Job was not updated.
        """
        sleep = None
        if wait:
            sleep = 10
        lock_filter, sort = self.generate_job_id_query(db_id, job_id, job_index)

        with self.lock_job(
            filter=lock_filter,
            break_lock=break_lock,
            sort=sort,
            sleep=sleep,
            max_wait=wait,
            get_locked_doc=True,
        ) as lock:
            doc = lock.locked_document
            if doc:
                if (
                    acceptable_states
                    and JobState(doc["state"]) not in acceptable_states
                ):
                    raise ValueError(
                        f"Job in state {doc['state']}. The action cannot be performed"
                    )
                values = dict(values)

                # Handle JSON field updates for job.* fields
                json_updates = {}
                regular_updates = {}

                for field, value in values.items():
                    if field.startswith("job."):
                        # This is a JSON field update
                        json_updates[field] = value
                    else:
                        # Regular column update
                        regular_updates[field] = value

                # If we have JSON updates, handle them specially
                if json_updates:
                    # Get the current job data
                    current_job = doc["job"]

                    # Apply updates to the JSON data
                    for field, value in json_updates.items():
                        # Parse the field path (e.g., "job.metadata.x" -> ["job", "metadata", "x"])
                        path_parts = field.split(".")
                        if path_parts[0] != "job":
                            continue

                        # Navigate and update the nested structure
                        current = current_job
                        for i, part in enumerate(path_parts[1:-1]):
                            if part not in current:
                                current[part] = {}
                            current = current[part]

                        # Set the final value
                        if len(path_parts) > 1:
                            current[path_parts[-1]] = value

                    # Add the updated job JSON to regular updates
                    regular_updates["job"] = current_job

                # Set the updates to be applied on lock release
                lock.update_on_release = regular_updates
                return doc["db_id"]

        return None

    def set_job_state(
        self,
        state: JobState,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        wait: int | None = None,
        break_lock: bool = False,
    ) -> str:
        """
        Set the state of a Job to an arbitrary JobState.
        Selected by db_id or uuid+index. Only one among db_id
        and job_id should be defined.

        No check is performed! Any job can be set to any state.
        Only for advanced users or for debugging purposes.

        Parameters
        ----------
        state
            The JobState to set.
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None the Job with the largest index
            will be selected.
        wait
            In case the Flow or Jobs that need to be updated are locked,
            wait this time (in seconds) for the lock to be released.
            Raise an error if lock is not released.
        break_lock
            Forcibly break the lock on locked documents.

        Returns
        -------
        str
            The db_id of the updated Job. None if the Job was not updated.
        """
        values = {
            "state": state.value,
            "remote.step_attempts": 0,
            "remote.retry_time_limit": None,
            "previous_state": None,
            "remote.queue_state": None,
            "remote.error": None,
            "error": None,
        }
        return self.set_job_doc_properties(
            values=values,
            job_id=job_id,
            db_id=db_id,
            job_index=job_index,
            wait=wait,
            break_lock=break_lock,
        )

    def get_jobs(
        self, query: dict, projection: list | dict | None = None
    ) -> list[dict]:
        """
        Get jobs as raw dictionaries based on query.

        Parameters
        ----------
        query
            Dictionary with query filters.
        projection
            Fields to include in the result.

        Returns
        -------
        list[dict]
            List of job documents as dictionaries.
        """
        with self.get_session() as session:
            # Convert MongoDB-style query to SQL conditions using converter
            conditions = MongoToSQLConverter.convert_query(query, SQLJob)

            # Build query
            query_stmt = select(SQLJob)
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            # Execute query
            jobs = session.execute(query_stmt).scalars().all()

            # Convert to dictionaries
            result = []
            for job in jobs:
                job_doc = job.to_job_doc()
                job_dict = job_doc.as_db_dict()

                # Apply projection if specified
                if projection:
                    if isinstance(projection, list):
                        # Include only specified fields
                        projected_dict = {}
                        for field in projection:
                            if "." in field:
                                # Handle nested fields like "remote.step_attempts"
                                parts = field.split(".")
                                if parts[0] in job_dict:
                                    if parts[0] not in projected_dict:
                                        projected_dict[parts[0]] = {}
                                    if (
                                        isinstance(job_dict[parts[0]], dict)
                                        and parts[1] in job_dict[parts[0]]
                                    ):
                                        projected_dict[parts[0]][parts[1]] = job_dict[
                                            parts[0]
                                        ][parts[1]]
                            elif field in job_dict:
                                projected_dict[field] = job_dict[field]
                        result.append(projected_dict)
                    elif isinstance(projection, dict):
                        # MongoDB-style projection {field: 1} or {field: 0}
                        if any(v == 0 for v in projection.values()):
                            # Exclusion projection
                            projected_dict = dict(job_dict)
                            for field, include in projection.items():
                                if include == 0 and field in projected_dict:
                                    del projected_dict[field]
                        else:
                            # Inclusion projection
                            projected_dict = {}
                            for field, include in projection.items():
                                if include == 1 and field in job_dict:
                                    projected_dict[field] = job_dict[field]
                        result.append(projected_dict)
                else:
                    result.append(job_dict)

            return result

    @contextlib.contextmanager
    def lock_job(
        self, filter: dict, sort: list[tuple[str, int] | str] | None = None, **kwargs
    ) -> Generator[SQLLock, None, None]:
        """
        Context manager to lock a job document.

        Parameters
        ----------
        filter
            Dictionary with query filters to select the job.
        sort
            Sort order for job selection.
        **kwargs
            Additional arguments passed to SQLLock.

        Yields
        ------
        SQLLock
            The lock object with the locked job document.
        """
        with self.get_session() as session:
            # Convert MongoDB-style filter to SQL conditions using converter
            conditions = MongoToSQLConverter.convert_query(filter, SQLJob)

            # Find the job to lock
            query_stmt = select(SQLJob)
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))
                # logger.debug(f"Applied WHERE clause with {len(conditions)} conditions")
            else:
                # logger.info(f"MongoToSQL: No conditions generated from filter: {filter}")
                pass

            # Apply sorting
            if sort:
                order_clauses = []
                for field, direction in sort:
                    if hasattr(SQLJob, field):
                        attr = getattr(SQLJob, field)
                        if direction == pymongo.DESCENDING:
                            order_clauses.append(attr.desc())
                        else:
                            order_clauses.append(attr.asc())
                if order_clauses:
                    query_stmt = query_stmt.order_by(*order_clauses)

            # Find first matching job (like MongoDB's find_one())
            # Use limit(1) + first() to handle potential duplicates gracefully
            # logger.debug(f"Executing query: {query_stmt}")
            result = session.execute(query_stmt.limit(1)).first()
            job = result[0] if result else None
            # logger.debug(f"Query returned job: {job.uuid if job else None}")

            if not job:
                # Create an empty lock for consistency with MongoDB version
                empty_lock = SQLLock(
                    session=session,
                    table_class=SQLJob,
                    filter={"id": -1},  # Non-existent ID
                    **kwargs,
                )
                empty_lock.locked_document = None
                yield empty_lock
                return

            # Create lock using the job's ID for atomic locking
            lock = SQLLock(
                session=session, table_class=SQLJob, filter={"id": job.id}, **kwargs
            )

            with lock:
                # locked_document is already a dictionary for compatibility with MongoDB API
                yield lock

    @contextlib.contextmanager
    def lock_job_for_update(
        self,
        query: dict,
        max_step_attempts: int,
        delta_retry: tuple[int, ...],
        next_step_delay: int | None = None,
        **kwargs,
    ) -> Generator[SQLLock, None, None]:
        """
        Lock a Job document for state update by the Runner.

        Parameters
        ----------
        query
            The query used to select the Job document to lock.
        max_step_attempts
            The maximum number of attempts for a single step after which
            the Job should be set to the REMOTE_ERROR state.
        delta_retry
            List of increasing delay between subsequent attempts when the
            advancement of a remote step fails. Used to set the retry time.
        next_step_delay
            An amount of seconds that sets the delay for the next step to
            start even in case there are no errors.
        **kwargs
            Additional arguments passed to lock_job.

        Yields
        ------
        SQLLock
            The lock object with the locked job document.
        """
        import traceback
        import warnings
        from datetime import timedelta

        from jobflow_remote.config.base import ConfigError
        from jobflow_remote.jobs.data import RemoteError

        db_filter = dict(query)
        db_filter["remote.retry_time_limit"] = {"$not": {"$gt": datetime.utcnow()}}

        if "sort" not in kwargs:
            kwargs["sort"] = [
                ("priority", pymongo.DESCENDING),
                ("created_on", pymongo.ASCENDING),
            ]

        with self.lock_job(
            filter=db_filter,
            **kwargs,
        ) as lock:
            doc = lock.locked_document
            no_retry = False
            error = None

            try:
                yield lock
            except ConfigError:
                error = traceback.format_exc()
                warnings.warn(error, stacklevel=2)
                no_retry = True
            except RemoteError as e:
                error = f"Remote error: {e.msg}"
                cause = e.__cause__
                if cause:
                    # this is required for support of python 3.9. In 3.10 the API
                    # changed and format_exception(e) could be used instead.
                    # Do that if/when support for 3.9 is dropped.
                    trace = traceback.format_exception(
                        type(cause), cause, cause.__traceback__
                    )
                    error += "\ncaused by:\n" + "".join(trace)
                no_retry = e.no_retry
            except Exception:
                error = traceback.format_exc()
                warnings.warn(error, stacklevel=2)

            set_output = lock.update_on_release

            if lock.locked_document:
                if not error:
                    next_step_time_limit = None
                    if next_step_delay:
                        next_step_time_limit = datetime.utcnow() + timedelta(
                            seconds=next_step_delay
                        )

                    # Success case - reset retry attempts
                    succeeded_update = {
                        "remote_step_attempts": 0,
                        "remote_retry_time_limit": next_step_time_limit,
                        "remote_error": None,
                    }

                    # Merge with any existing updates
                    if set_output:
                        if "$set" in set_output:
                            update_on_release = {
                                "$set": {**succeeded_update, **set_output["$set"]}
                            }
                        else:
                            update_on_release = {
                                "$set": {**succeeded_update, **set_output}
                            }
                    else:
                        update_on_release = {"$set": succeeded_update}
                else:
                    # Error case - handle retry logic
                    if isinstance(doc, dict):
                        step_attempts = doc.get("remote", {}).get("step_attempts", 0)
                        doc_state = doc.get("state")
                    else:
                        # doc is JobDoc pydantic model
                        step_attempts = doc.remote.step_attempts
                        doc_state = doc.state.value

                    no_retry = no_retry or step_attempts >= max_step_attempts

                    try:
                        # Attempt to get queue files (implement if needed)
                        queue_out, queue_err = (
                            None,
                            None,
                        )  # TODO: implement _get_downloaded_queue_files
                    except Exception:
                        logger.warning(
                            "Error while trying to retrieve queue output", exc_info=True
                        )
                        queue_out, queue_err = None, None

                    if no_retry:
                        # Set to REMOTE_ERROR state
                        update_on_release = {
                            "$set": {
                                "state": JobState.REMOTE_ERROR.value,
                                "previous_state": doc_state,
                                "remote_error": error,
                                "remote_queue_out": queue_out,
                                "remote_queue_err": queue_err,
                            }
                        }
                    else:
                        # Increment retry attempts and set retry time
                        step_attempts += 1
                        ind = min(step_attempts, len(delta_retry)) - 1
                        delta = delta_retry[ind]
                        retry_time_limit = datetime.utcnow() + timedelta(seconds=delta)

                        update_on_release = {
                            "$set": {
                                "remote_step_attempts": step_attempts,
                                "remote_retry_time_limit": retry_time_limit,
                                "remote_error": error,
                                "remote_queue_out": queue_out,
                                "remote_queue_err": queue_err,
                            }
                        }

                # Convert MongoDB-style $set updates to direct field updates for SQLLock
                if update_on_release and "$set" in update_on_release:
                    lock.update_on_release = update_on_release["$set"]
                else:
                    lock.update_on_release = update_on_release or {}

    def ping_running_runner(self) -> bool:
        """
        Ping the running_runner document, if exists and has been activated as a daemon.

        Returns
        -------
        bool
            True if the ping was successful.
        """
        with self.get_session() as session:
            try:
                # First, find the existing document
                aux_doc = session.execute(
                    select(SQLAuxiliary).where(SQLAuxiliary.doc_id == "running_runner")
                ).scalar_one_or_none()

                if not aux_doc:
                    return False

                # With JSON column, document is already deserialized
                document_data = aux_doc.document

                # Check if running_runner.last_pinged exists (equivalent to MongoDB query)
                if (
                    not isinstance(document_data, dict)
                    or "running_runner" not in document_data
                ):
                    return False

                running_runner = document_data["running_runner"]
                if (
                    not isinstance(running_runner, dict)
                    or "last_pinged" not in running_runner
                ):
                    return False

                # Update only the last_pinged field, preserving all other data
                running_runner["last_pinged"] = (
                    datetime.utcnow()
                )  # Keep as datetime, MontyEncoder will handle it
                document_data["running_runner"] = running_runner

                # Update the document - SQLAlchemy will handle serialization automatically
                result = session.execute(
                    update(SQLAuxiliary)
                    .where(SQLAuxiliary.doc_id == "running_runner")
                    .values(document=document_data, updated_on=datetime.utcnow())
                    .returning(SQLAuxiliary.id)
                )

                ping_result = result.first()
                session.commit()

                return ping_result is not None

            except Exception:
                session.rollback()
                return False

    @contextlib.contextmanager
    def lock_auxiliary(self, **kwargs) -> Generator[SQLLock, None, None]:
        """
        Context manager to lock a document in the auxiliary collection.

        Parameters
        ----------
        **kwargs
            Arguments passed to SQLLock.

        Yields
        ------
        SQLLock
            The lock object with the locked auxiliary document.
        """
        with self.get_session() as session:
            lock = SQLLock(session=session, table_class=SQLAuxiliary, **kwargs)

            with lock:
                yield lock

    def set_job_run_properties(
        self,
        worker: str | None = None,
        exec_config: str | dict | None = None,  # ExecutionConfig import not available
        resources: dict | None = None,  # QResources import not available
        priority: int | None = None,
        update: bool = True,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
        custom_query: dict | None = None,
        raise_on_error: bool = True,
    ) -> list[str]:
        """
        Set execution properties for selected Jobs:
        worker, exec_config and resources.

        Parameters
        ----------
        worker
            The name of the worker to set.
        exec_config
            The name of the exec_config to set or an explicit value as dict.
        resources
            The resources to be set as a dict.
        priority
            The priority of the Job.
        update
            If True, when setting exec_config and resources a passed dictionary
            will be used to update already existing values.
            If False it will replace the original values.
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
        states
            One or more states of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        workers
            One or more worker names.
        custom_query
            A generic query. Keys must not overlap with other specified query options.
        raise_on_error
            If True raise in case of error on one job error and stop the loop.
            Otherwise, just log the error and proceed.

        Returns
        -------
        list[str]
            List of db_ids of the updated Jobs.
        """
        import json

        set_dict: dict = {}

        if worker:
            if self.project and worker not in self.project.workers:
                raise ValueError(f"worker {worker} is not present in the project")
            set_dict["worker"] = worker

        if exec_config:
            if (
                isinstance(exec_config, str)
                and self.project
                and exec_config not in self.project.exec_config
            ):
                raise ValueError(
                    f"exec_config {exec_config} is not present in the project"
                )

            # For SQL, we need to handle merge logic per job
            if update and isinstance(exec_config, dict):
                # Mark for merge - will be handled in the inline function
                set_dict["_exec_config_merge"] = exec_config
            elif isinstance(exec_config, dict):
                set_dict["exec_config"] = json.dumps(exec_config)
            elif hasattr(exec_config, "model_dump"):  # Pydantic models
                set_dict["exec_config"] = json.dumps(exec_config.model_dump())
            elif hasattr(exec_config, "as_dict"):  # Objects with as_dict method
                set_dict["exec_config"] = json.dumps(exec_config.as_dict())
            else:
                # Plain string or other serializable type
                set_dict["exec_config"] = exec_config

        if resources:
            # For SQL, we need to handle merge logic per job
            if update and isinstance(resources, dict):
                # Mark for merge - will be handled in the inline function
                set_dict["_resources_merge"] = resources
            elif isinstance(resources, dict):
                set_dict["resources"] = json.dumps(resources)
            elif hasattr(resources, "model_dump"):  # Pydantic models
                set_dict["resources"] = json.dumps(resources.model_dump())
            elif hasattr(resources, "as_dict"):  # Objects with as_dict method
                set_dict["resources"] = json.dumps(resources.as_dict())
            else:
                # Try to serialize the object directly
                try:
                    set_dict["resources"] = json.dumps(resources, default=str)
                except (TypeError, ValueError):
                    # If all else fails, convert to string
                    set_dict["resources"] = str(resources)

        if priority is not None:
            set_dict["priority"] = priority

        acceptable_states = [
            JobState.READY,
            JobState.WAITING,
            JobState.COMPLETED,
            JobState.FAILED,
            JobState.PAUSED,
            JobState.REMOTE_ERROR,
        ]

        def set_job_run_properties_with_merge(db_id: str, **kwargs) -> str:
            """Inner function to handle merge logic for exec_config and resources."""
            values = kwargs.get("values", {})
            acceptable_states = kwargs.get("acceptable_states")

            # Handle merge logic for exec_config and resources
            if "_exec_config_merge" in values or "_resources_merge" in values:
                # Get current job to merge values
                job_doc = self.get_job_doc(db_id=db_id)
                if not job_doc:
                    return None

                final_values = dict(values)

                # Handle exec_config merge
                if "_exec_config_merge" in final_values:
                    merge_config = final_values.pop("_exec_config_merge")
                    current_config = job_doc.exec_config or {}

                    if isinstance(current_config, str):
                        # If current is string, replace entirely
                        final_values["exec_config"] = json.dumps(merge_config)
                    elif isinstance(current_config, dict):
                        # Current is already a dictionary
                        merged_config = {**current_config, **merge_config}
                        final_values["exec_config"] = json.dumps(merged_config)
                    else:
                        # Current is an object (like ExecutionConfig), convert to dict first
                        if hasattr(current_config, "model_dump"):
                            current_dict = current_config.model_dump()
                        elif hasattr(current_config, "as_dict"):
                            current_dict = current_config.as_dict()
                        else:
                            # Fallback: use the merge_config directly
                            current_dict = {}

                        merged_config = {**current_dict, **merge_config}
                        final_values["exec_config"] = json.dumps(merged_config)

                # Handle resources merge
                if "_resources_merge" in final_values:
                    merge_resources = final_values.pop("_resources_merge")
                    current_resources = job_doc.resources or {}
                    merged_resources = {**current_resources, **merge_resources}
                    final_values["resources"] = json.dumps(merged_resources)

                values = final_values

            return self.set_job_doc_properties(
                values=values,
                db_id=db_id,
                acceptable_states=acceptable_states,
                use_pipeline=update,
            )

        return self._many_jobs_action(
            method=set_job_run_properties_with_merge,
            action_description="setting job run properties",
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=custom_query,
            raise_on_error=raise_on_error,
            values=set_dict,
            acceptable_states=acceptable_states,
        )

    def delete_flow(
        self,
        flow_id: str,
        delete_output: bool = False,
        delete_files: bool = False,
        cancel_processes: bool = True,
    ) -> bool:
        """
        Delete a single Flow based on the uuid.

        Parameters
        ----------
        flow_id
            One Flow ids. Can be db_id or uuid.
        delete_output
            If True also delete the associated output in the JobStore.
        delete_files
            If True also delete the files on the worker.
        cancel_processes
            If True will attempt to delete the processes for SUBMITTED and RUNNING jobs.
            Failure to cancel will not stop the deletion of the Flow.

        Returns
        -------
        bool
            True if the flow has been deleted.
        """
        flow = self.get_flow_info_by_flow_uuid(flow_id)
        if not flow:
            return False

        job_uuids = flow["jobs"]

        if delete_output:
            jobstore = self.jobstore
            if jobstore_name := self.get_flow_store(flow_id):
                jobstore = self.optional_jobstores[jobstore_name]
            jobstore.remove_docs({"uuid": {"$in": job_uuids}})

        if delete_files or cancel_processes:
            jobs_info = self.get_jobs_info(flow_ids=[flow_id])
            if cancel_processes:
                from jobflow_remote.jobs.state import JobState

                for ji in jobs_info:
                    if ji.state in [JobState.SUBMITTED, JobState.RUNNING]:
                        # try cancelling the job submitted to the remote queue
                        try:
                            self._cancel_queue_process(ji.model_dump(mode="python"))
                        except Exception:
                            logger.warning(
                                f"Failed cancelling the process for Job {ji.uuid} {ji.index} while deleting Flow {flow_id}",
                                exc_info=True,
                            )
            # delete files after cancelling the queue job
            if delete_files:
                self._safe_delete_files(jobs_info)

        with self.get_session() as session:
            # Delete all jobs in the flow
            session.execute(delete(SQLJob).where(SQLJob.uuid.in_(job_uuids)))

            # Delete the flow
            result = session.execute(delete(SQLFlow).where(SQLFlow.uuid == flow_id))

            session.commit()
            return result.rowcount > 0

    def delete_flows(
        self,
        flow_ids: str | list[str] | None = None,
        max_limit: int = 10,
        delete_output: bool = False,
        delete_files: bool = False,
        cancel_processes: bool = True,
    ) -> int:
        """
        Delete a list of Flows based on the flow uuids.

        Parameters
        ----------
        flow_ids
            One or more Flow uuids.
        max_limit
            The Flows will be deleted only if the total number is lower than the
            specified limit. 0 means no limit.
        delete_output
            If True also delete the associated output in the JobStore.
        delete_files
            If True also delete the files on the worker.
        cancel_processes
            If True will attempt to delete the processes for SUBMITTED and RUNNING jobs.
            Failure to cancel will not stop the deletion of the Flow.

        Returns
        -------
        int
            Number of deleted flows.
        """
        if isinstance(flow_ids, str):
            flow_ids = [flow_ids]

        if flow_ids is None:
            with self.get_session() as session:
                result = session.execute(select(SQLFlow.uuid)).all()
                flow_ids = [row[0] for row in result]

        if max_limit != 0 and len(flow_ids) > max_limit:
            raise ValueError(
                f"Cannot delete {len(flow_ids)} Flows as they exceed the specified maximum "
                f"limit ({max_limit}). Increase the limit to delete the Flows."
            )

        deleted_count = 0
        for flow_id in flow_ids:
            if self.delete_flow(
                flow_id=flow_id,
                delete_output=delete_output,
                delete_files=delete_files,
                cancel_processes=cancel_processes,
            ):
                deleted_count += 1

        return deleted_count

    def count_jobs_states(
        self, states: list[JobState], worker: str | None = None
    ) -> dict[JobState, int]:
        """
        Count the number of jobs in each of the given states.

        Parameters
        ----------
        states
            List of JobState to count.
        worker
            Name of the worker

        Returns
        -------
        dict[JobState, int]
            A dictionary with the count of jobs in each state.
        """
        with self.get_session() as session:
            # Build query conditions
            conditions = [SQLJob.state.in_([s.value for s in states])]
            if worker:
                conditions.append(SQLJob.worker == worker)

            # Execute aggregation query
            result = session.execute(
                select(SQLJob.state, func.count(SQLJob.state).label("count"))
                .where(and_(*conditions))
                .group_by(SQLJob.state)
            ).all()

            # Build output dictionary
            out = {}
            for row in result:
                out[JobState(row.state)] = row.count

            # Ensure all requested states are in the output (with 0 if not found)
            for state in states:
                out[state] = out.get(state, 0)

            return out

    def count_flows_states(self, states: list[FlowState]) -> dict[FlowState, int]:
        """
        Count the number of flows in each of the given states.

        Parameters
        ----------
        states
            List of FlowState to count.

        Returns
        -------
        dict[FlowState, int]
            A dictionary with the count of flows in each state.
        """
        with self.get_session() as session:
            # Execute aggregation query
            result = session.execute(
                select(SQLFlow.state, func.count(SQLFlow.state).label("count"))
                .where(SQLFlow.state.in_([s.value for s in states]))
                .group_by(SQLFlow.state)
            ).all()

            # Build output dictionary
            out = {}
            for row in result:
                out[FlowState(row.state)] = row.count

            # Ensure all requested states are in the output (with 0 if not found)
            for state in states:
                out[state] = out.get(state, 0)

            return out

    def get_running_runner(self) -> dict | str:
        """Get the running runner information from the auxiliary collection."""
        with self.get_session() as session:
            # Find the specific running_runner document
            aux_doc = session.execute(
                select(SQLAuxiliary).where(SQLAuxiliary.doc_id == "running_runner")
            ).scalar_one_or_none()

            if not aux_doc:
                return "NO_DOCUMENT"

            # With JSON column, document is already deserialized
            data = aux_doc.document

            if isinstance(data, dict) and "running_runner" in data:
                return data["running_runner"]
            else:
                return "NO_DOCUMENT"

    def get_job_output(
        self,
        job_id: str | None = None,
        db_id: str | None = None,
        job_index: int | None = None,
        load=False,
    ):
        """
        Get the output of a single Job based on db_id or uuid+index.
        Only one among db_id and job_id should be defined.

        Parameters
        ----------
        db_id
            The db_id of the Job.
        job_id
            The uuid of the Job.
        job_index
            The index of the Job. If None the Job with the largest index
            will be selected.
        load
            Which items to load from additional stores. Setting to ``True`` will load
            all items stored in additional stores. See the ``JobStore`` constructor for
            more details.

        Returns
        -------
        Any
            The output(s) for the job
        """
        job_info = None
        if db_id:
            job_info = self.get_job_info(
                job_id=job_id,
                job_index=job_index,
                db_id=db_id,
            )
            if not job_info:
                raise ValueError(f"No Job with db_id {db_id}")
            job_id = job_info.uuid
            job_index = job_info.index

        jobstore = self.jobstore
        # if jobstore are defined need to check which JobStore to use
        if self.optional_jobstores:
            if not job_info:
                job_info = self.get_job_info(
                    job_id=job_id,
                    job_index=job_index,
                )
                if not job_info:
                    raise ValueError(
                        "The Job is not present in the Queue DB, cannot "
                        "determine the store to fetch the output"
                    )
            jobstore_name = self.get_flow_store(job_info.hosts[-1])
            if jobstore_name:
                jobstore = self.optional_jobstores[jobstore_name]
        return jobstore.get_output(job_id, job_index or "last", load=load)

    def _safe_delete_files(
        self, jobs_info: Sequence[JobInfo | dict]
    ) -> list[JobInfo | dict]:
        """
        Delete the files associated to the selected Jobs.

        Checks that the folder to be deleted contains the jfremote_in.json
        file to avoid mistakenly deleting other folders.

        Parameters
        ----------
        jobs_info
            A list of JobInfo whose files should be deleted.

        Returns
        -------
        list
            The list of JobInfo whose files have been actually deleted.
        """
        deleted = []
        with SharedHosts(self.project) as shared_hosts:
            for job_info in jobs_info:
                if isinstance(job_info, JobInfo):
                    run_dir = job_info.run_dir
                    worker = job_info.worker
                else:
                    run_dir = job_info["run_dir"]
                    worker = job_info["worker"]
                if run_dir:
                    host = shared_hosts.get_host(worker)
                    if safe_remove_job_files(
                        host=host, run_dir=run_dir, raise_on_error=False
                    ):
                        deleted.append(job_info)
        return deleted

    def count_flows(
        self,
        query: dict | None = None,
        job_ids: str | list[str] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: FlowState | list[FlowState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        locked: bool = False,
    ) -> int:
        """
        Count flows based on filter parameters.

        Parameters
        ----------
        query
            A generic query. Will override all the other parameters.
        job_ids
            One or more strings with uuids of Jobs belonging to the Flow.
        db_ids
            One or more db_ids of Jobs belonging to the Flow.
        flow_ids
            One or more Flow uuids.
        states
            One or more states of the Flows.
        start_date
            Filter Flows that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Flows that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Flow. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        locked
            If True only locked Flows will be counted.

        Returns
        -------
        int
            Number of Flows matching the criteria.
        """
        if query:
            # If custom query provided, use it directly with MongoToSQLConverter
            with self.get_session() as session:
                conditions = MongoToSQLConverter.convert_query(query, SQLFlow)

                from sqlalchemy import func

                query_stmt = select(func.count(SQLFlow.id))
                if conditions:
                    query_stmt = query_stmt.where(and_(*conditions))

                result = session.execute(query_stmt).scalar()
                return result or 0

        # Use consolidated flow query building method
        conditions, flow_uuids_from_jobs = self._build_query_flow(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            locked=locked,
        )

        # Early return if no matching flows
        if flow_uuids_from_jobs is not None and len(flow_uuids_from_jobs) == 0:
            return 0

        with self.get_session() as session:
            # Build count query
            from sqlalchemy import func

            query_stmt = select(func.count(SQLFlow.id))
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            # Execute count query
            result = session.execute(query_stmt).scalar()
            return result or 0

    def clean_running_runner(self, break_lock: bool = False) -> None:
        """
        Clean the running_runner document in the auxiliary collection.

        This method removes any running runner state by setting the running_runner
        field to None in the auxiliary document.

        Parameters
        ----------
        break_lock
            If True, forcibly break any existing lock on the document.
        """

        db_filter = {"running_runner": {"$exists": True}}
        with self.lock_auxiliary(
            filter=db_filter, break_lock=break_lock, get_locked_doc=True
        ) as lock:
            if not lock.locked_document:
                if lock.unavailable_document:
                    raise LockedDocumentError(
                        "Document for the running runner in the auxiliary collection is locked"
                    )
                raise MissingDocumentError(
                    "Runner document missing from auxiliary collection"
                )

            # Set running_runner field to None using MongoDB-style field updates
            # SQLLock.__exit__ will handle extracting the current document and applying the field update
            lock.update_on_release = {"running_runner": None}

    def unlock_jobs(
        self,
        job_ids: tuple[str, int] | list[tuple[str, int]] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: JobState | list[JobState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
        metadata: dict | None = None,
        workers: str | list[str] | None = None,
    ) -> int:
        """
        Forcibly remove the lock on a locked Job document.
        This should be used only if a lock is a leftover of a process that is not
        running anymore. Doing otherwise may result in inconsistencies.

        Parameters
        ----------
        job_ids
            One or more tuples, each containing the (uuid, index) pair of the
            Jobs to retrieve.
        db_ids
            One or more db_ids of the Jobs to retrieve.
        flow_ids
            One or more Flow uuids to which the Jobs to retrieve belong. Can contain db_ids and uuids.
        states
            One or more states of the Jobs.
        start_date
            Filter Jobs that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Jobs that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Job. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)
        metadata
            A dictionary of the values of the metadata to match. Should be an
            exact match for all the values provided.
        workers
            One or more worker names.

        Returns
        -------
        int
            Number of modified Jobs.
        """
        query_dict = self._build_query_job(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            locked=True,  # Only locked jobs
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=metadata,
            workers=workers,
            custom_query=None,
        )

        with self.get_session() as session:
            # Convert MongoDB-style query to SQL conditions
            conditions = MongoToSQLConverter.convert_query(query_dict, SQLJob)

            # Build update statement
            update_stmt = (
                update(SQLJob)
                .where(and_(*conditions))
                .values(
                    lock_id=None,
                    lock_time=None,
                    updated_on=datetime.utcnow(),
                )
            )

            result = session.execute(update_stmt)
            session.commit()
            return result.rowcount

    def unlock_flows(
        self,
        job_ids: str | list[str] | None = None,
        db_ids: str | list[str] | None = None,
        flow_ids: str | list[str] | None = None,
        states: FlowState | list[FlowState] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        name: str | None = None,
    ) -> int:
        """
        Forcibly remove the lock on a locked Flow document.
        This should be used only if a lock is a leftover of a process that is not
        running anymore. Doing otherwise may result in inconsistencies.

        Parameters
        ----------
        job_ids
            One or more strings with uuids of Jobs belonging to the Flow.
        db_ids
            One or more db_ids of Jobs belonging to the Flow.
        flow_ids
            One or more Flow uuids.
        states
            One or more states of the Flows.
        start_date
            Filter Flows that were updated_on after this date.
            Should be in the machine local time zone. It will be converted to UTC.
        end_date
            Filter Flows that were updated_on before this date.
            Should be in the machine local time zone. It will be converted to UTC.
        name
            Pattern matching the name of Flow. Default is an exact match, but all
            conventions from python fnmatch can be used (e.g. *test*)

        Returns
        -------
        int
            Number of modified Flows.
        """
        # Use consolidated flow query building method (force locked=True for unlock operation)
        conditions, flow_uuids_from_jobs = self._build_query_flow(
            job_ids=job_ids,
            db_ids=db_ids,
            flow_ids=flow_ids,
            states=states,
            start_date=start_date,
            end_date=end_date,
            name=name,
            metadata=None,  # unlock_flows doesn't support metadata filtering
            locked=True,  # Only locked flows for unlock operation
        )

        # Early return if no matching flows
        if flow_uuids_from_jobs is not None and len(flow_uuids_from_jobs) == 0:
            return 0

        with self.get_session() as session:
            # Build update statement
            update_stmt = (
                update(SQLFlow)
                .where(and_(*conditions))
                .values(
                    lock_id=None,
                    lock_time=None,
                    updated_on=datetime.utcnow(),
                )
            )

            result = session.execute(update_stmt)
            session.commit()
            return result.rowcount

    def unlock_runner(self) -> tuple[int, int]:
        """
        Forcibly remove the lock on a locked Runner document.
        This should be used only if a lock is a leftover of a process that is not
        running anymore. Should also be done only when no daemon is running.
        Doing otherwise may result in inconsistencies.

        Returns
        -------
        tuple
            Number of runner documents and modified runner documents.
        """
        with self.get_session() as session:
            # Step 1: Count all documents with running_runner field (matched_count)
            exists_conditions = MongoToSQLConverter.convert_query(
                {"running_runner": {"$exists": True}}, SQLAuxiliary
            )

            from sqlalchemy import func

            matched_count = (
                session.execute(
                    select(func.count(SQLAuxiliary.id)).where(and_(*exists_conditions))
                ).scalar()
                or 0
            )

            # Step 2: Update only those that are actually locked (modified_count)
            locked_conditions = MongoToSQLConverter.convert_query(
                {"running_runner": {"$exists": True}}, SQLAuxiliary
            )
            locked_conditions.append(SQLAuxiliary.lock_id.is_not(None))

            update_stmt = (
                update(SQLAuxiliary)
                .where(and_(*locked_conditions))
                .values(
                    lock_id=None,
                    lock_time=None,
                    updated_on=datetime.utcnow(),
                )
            )

            result = session.execute(update_stmt)
            session.commit()

            return matched_count, result.rowcount

    # =============================================================================
    # Testing Compatibility Properties
    # =============================================================================

    @property
    def jobs(self):
        """MongoDB-like collection interface for jobs - for testing compatibility."""
        return SQLCollectionInterface(self, SQLJob, "jobs")

    @property
    def flows(self):
        """MongoDB-like collection interface for flows - for testing compatibility."""
        return SQLCollectionInterface(self, SQLFlow, "flows")

    @property
    def auxiliary(self):
        """MongoDB-like collection interface for auxiliary - for testing compatibility."""
        return SQLCollectionInterface(self, SQLAuxiliary, "auxiliary")


class SQLCollectionInterface:
    """
    Provides MongoDB-like collection interface for testing compatibility.

    This class makes SQL tables behave like MongoDB collections, allowing
    existing tests to work without modification.
    """

    def __init__(self, controller: SQLJobController, table_class, collection_name: str):
        self.controller = controller
        self.table_class = table_class
        self.collection_name = collection_name

    def find_one(self, query: dict = None, projection: dict = None):
        """Find one document matching the query."""
        query = query or {}

        with self.controller.get_session() as session:
            conditions = MongoToSQLConverter.convert_query(query, self.table_class)

            query_stmt = select(self.table_class)
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            row = session.execute(query_stmt).scalar_one_or_none()

            if row:
                if hasattr(row, "to_job_doc"):
                    doc_dict = row.to_job_doc().as_db_dict()
                elif hasattr(row, "to_flow_doc"):
                    doc_dict = row.to_flow_doc().as_db_dict()
                else:
                    # Auxiliary documents
                    doc_dict = row.to_dict()

                # Apply projection if specified
                if projection:
                    return self._apply_projection(doc_dict, projection)
                return doc_dict

            return None

    def find(
        self,
        query: dict = None,
        projection: dict = None,
        sort: list = None,
        limit: int = None,
    ):
        """Find documents matching the query."""
        query = query or {}

        with self.controller.get_session() as session:
            conditions = MongoToSQLConverter.convert_query(query, self.table_class)

            query_stmt = select(self.table_class)
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            # Apply sorting
            if sort:
                order_clauses = []
                for field, direction in sort:
                    if hasattr(self.table_class, field):
                        attr = getattr(self.table_class, field)
                        if direction == -1:  # pymongo.DESCENDING
                            order_clauses.append(attr.desc())
                        else:
                            order_clauses.append(attr.asc())
                if order_clauses:
                    query_stmt = query_stmt.order_by(*order_clauses)

            # Apply limit
            if limit:
                query_stmt = query_stmt.limit(limit)

            rows = session.execute(query_stmt).scalars().all()

            results = []
            for row in rows:
                if hasattr(row, "to_job_doc"):
                    doc_dict = row.to_job_doc().as_db_dict()
                elif hasattr(row, "to_flow_doc"):
                    doc_dict = row.to_flow_doc().as_db_dict()
                else:
                    # Auxiliary documents
                    doc_dict = row.to_dict()

                # Apply projection if specified
                if projection:
                    doc_dict = self._apply_projection(doc_dict, projection)

                results.append(doc_dict)

            return results

    def count_documents(self, query: dict = None) -> int:
        """Count documents matching the query."""
        query = query or {}

        with self.controller.get_session() as session:
            conditions = MongoToSQLConverter.convert_query(query, self.table_class)

            from sqlalchemy import func

            query_stmt = select(func.count(self.table_class.id))
            if conditions:
                query_stmt = query_stmt.where(and_(*conditions))

            result = session.execute(query_stmt).scalar()
            return result or 0

    def insert_one(self, document: dict):
        """Insert one document."""
        with self.controller.get_session() as session:
            if self.collection_name == "jobs":
                job_doc = JobDoc.model_validate(document)
                sql_obj = SQLJob.from_job_doc(job_doc)
            elif self.collection_name == "flows":
                flow_doc = FlowDoc.model_validate(document)
                sql_obj = SQLFlow.from_flow_doc(flow_doc)
            else:  # auxiliary
                doc_id = document.get("_id") or document.get("doc_id")

                # If no explicit doc_id, infer from document structure
                if not doc_id:
                    # For auxiliary documents like {"running_runner": None} or {"next_id": 1}
                    # Use the key as doc_id
                    aux_keys = [
                        k
                        for k in document
                        if k
                        not in [
                            "_id",
                            "doc_id",
                            "lock_id",
                            "lock_time",
                            "created_on",
                            "updated_on",
                        ]
                    ]
                    if len(aux_keys) == 1:
                        doc_id = aux_keys[0]
                    else:
                        raise ValueError(
                            f"Cannot infer doc_id from auxiliary document: {document}"
                        )

                sql_obj = SQLAuxiliary.from_dict(doc_id, document)

            session.add(sql_obj)
            session.commit()

            # Return MongoDB-like result
            return type(
                "InsertResult",
                (),
                {"inserted_id": document.get("_id") or document.get("db_id", doc_id)},
            )()

    def insert_many(self, documents: list[dict]):
        """Insert multiple documents."""
        inserted_ids = []

        with self.controller.get_session() as session:
            for document in documents:
                if self.collection_name == "jobs":
                    job_doc = JobDoc.model_validate(document)
                    sql_obj = SQLJob.from_job_doc(job_doc)
                    inserted_ids.append(document.get("_id", document.get("db_id")))
                elif self.collection_name == "flows":
                    flow_doc = FlowDoc.model_validate(document)
                    sql_obj = SQLFlow.from_flow_doc(flow_doc)
                    inserted_ids.append(document.get("_id", document.get("uuid")))
                else:  # auxiliary
                    doc_id = document.get("_id") or document.get("doc_id")
                    sql_obj = SQLAuxiliary.from_dict(doc_id, document)
                    inserted_ids.append(doc_id)

                session.add(sql_obj)

            session.commit()

        # Return MongoDB-like result
        return type("InsertResult", (), {"inserted_ids": inserted_ids})()

    def update_one(self, query: dict, update: dict, upsert: bool = False):
        """Update one document matching the query."""
        with self.controller.get_session() as session:
            conditions = MongoToSQLConverter.convert_query(query, self.table_class)

            # Extract update operations
            set_values = {}
            if "$set" in update:
                set_values = MongoToSQLConverter.convert_field_names_for_update(
                    update["$set"], self.table_class
                )
            else:
                set_values = MongoToSQLConverter.convert_field_names_for_update(
                    update, self.table_class
                )

            from sqlalchemy import update as sql_update_func

            update_stmt = (
                sql_update_func(self.table_class)
                .where(and_(*conditions))
                .values(**set_values)
            )

            result = session.execute(update_stmt)
            session.commit()

            # Return MongoDB-like result
            return type(
                "UpdateResult",
                (),
                {"matched_count": result.rowcount, "modified_count": result.rowcount},
            )()

    def update_many(self, query: dict, update: dict):
        """Update multiple documents matching the query."""
        return self.update_one(query, update)  # Same implementation for SQL

    def delete_one(self, query: dict):
        """Delete one document matching the query."""
        with self.controller.get_session() as session:
            conditions = MongoToSQLConverter.convert_query(query, self.table_class)

            delete_stmt = delete(self.table_class).where(and_(*conditions))
            result = session.execute(delete_stmt)
            session.commit()

            # Return MongoDB-like result
            return type(
                "DeleteResult", (), {"deleted_count": min(result.rowcount, 1)}
            )()

    def delete_many(self, query: dict):
        """Delete multiple documents matching the query."""
        with self.controller.get_session() as session:
            conditions = MongoToSQLConverter.convert_query(query, self.table_class)

            delete_stmt = delete(self.table_class).where(and_(*conditions))
            result = session.execute(delete_stmt)
            session.commit()

            # Return MongoDB-like result
            return type("DeleteResult", (), {"deleted_count": result.rowcount})()

    def drop(self):
        """Drop the collection (table)."""
        self.table_class.__table__.drop(self.controller.engine, checkfirst=True)
        Base.metadata.create_all(bind=self.controller.engine)  # Recreate empty table

    def _apply_projection(self, doc: dict, projection: dict) -> dict:
        """Apply MongoDB-style projection to document."""
        if not projection:
            return doc

        # Check if it's inclusion or exclusion projection
        is_inclusion = any(v == 1 for v in projection.values() if v != 0)

        if is_inclusion:
            # Inclusion projection - only include specified fields
            result = {}
            for field, include in projection.items():
                if include == 1 and field in doc:
                    result[field] = doc[field]
            # Always include _id unless explicitly excluded
            if "_id" not in projection and "_id" in doc:
                result["_id"] = doc["_id"]
            return result
        else:
            # Exclusion projection - exclude specified fields
            result = dict(doc)
            for field, exclude in projection.items():
                if exclude == 0 and field in result:
                    del result[field]
            return result
