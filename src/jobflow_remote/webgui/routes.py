from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from math import ceil
from zoneinfo import ZoneInfo
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Form, HTTPException, Query, Request, APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from bson import ObjectId

from jobflow_remote import ConfigManager
from jobflow_remote.jobs.daemon import DaemonManager, DaemonStatus
from jobflow_remote.jobs.graph import get_mermaid
from jobflow_remote.jobs.jobcontroller import JobController
from jobflow_remote.jobs.report import JobsReport
from jobflow_remote.jobs.state import JobState
from fastapi import Body

router = APIRouter()

cm = ConfigManager()
list_projects = list(cm.projects.keys())

job_controllers: Dict[str, JobController] = {}
daemon_managers: Dict[str, DaemonManager] = {}
job_controller: Optional[JobController] = None
job_controller_actions: Optional[Dict[str, Dict[str, Any]]] = None
daemon_manager: Optional[DaemonManager] = None
jfreport: Optional[JobsReport] = None

class ActionEnum(str, Enum):
    rerun = "Rerun"
    play = "Play"
    pause = "Pause"
    stop = "Stop"
    retry = "Retry"
    delete = "Delete"

class WhatEnum(str, Enum):
    jobs = "jobs"
    flows = "flows"

class ActionConfirmationResponse(BaseModel):
    action: str
    what: str
    response: Any
    delete_output: Optional[bool] = None
    delete_files: Optional[bool] = None

class RunnerStatusResponse(BaseModel):
    proj_name: str
    status: str
    color: str
    start_stop_btn_lbl: Dict[str, str]

class SumReportResponse(BaseModel):
    what: str
    total_jobs: int
    most_common_state: str
    most_common_count: int
    most_common_percentage: str
    running_count: int
    completed_count: int
    error_count: int
    active_count: int
    longest_running: Any
    worker_utilization: Any

class TrendData(BaseModel):
    dates: List[str]
    completed: List[int]
    failed: List[int]
    remote_error: List[int]

class TrendsResponse(BaseModel):
    what: str
    trend_data: TrendData

class StateData(BaseModel):
    state: str
    count: int
    percentage: str

class StateDistroResponse(BaseModel):
    what: str
    state_data: List[StateData]

class JobFlowInfoResponse(BaseModel):
    jf_id: str
    what: str
    info: Dict[str, Any]


class QueryResponse(BaseModel):
    proj_name: str
    what: str
    all_entries: List[Dict[str, Any]]
    sort_by: str
    next_sort_order: int
    action_btns_lbl: Tuple[str, ...]
    page: int
    total_pages: int
    entries_per_page: int
    db_id: Optional[str] = None
    uuid: Optional[str] = None
    name: Optional[str] = None
    state: Optional[str] = None
    worker: Optional[str] = None
    start_date: Optional[str] = None
    start_time: Optional[str] = None
    end_date: Optional[str] = None
    end_time: Optional[str] = None
    timezone_in: Optional[str] = None
    total_entries: int

def set_job_controller_deamon(project_name: str):
    global job_controller, job_controller_actions, daemon_manager

    if project_name not in job_controllers:
        job_controllers[project_name] = JobController.from_project_name(
            project_name=project_name
        )
        daemon_managers[project_name] = DaemonManager.from_project(
            cm.get_project(project_name)
        )

    daemon_manager = daemon_managers[project_name]
    job_controller = job_controllers[project_name]
    job_controller_actions = {
        "jobs": {
            "Play": job_controller.play_jobs,
            "Pause": job_controller.pause_jobs,
            "Stop": job_controller.stop_jobs,
            "Retry": job_controller.retry_jobs,
            "Rerun": job_controller.rerun_jobs,
        },
        "flows": {
            "Delete": job_controller.delete_flows,
        },
    }

def update_jfreport(project_name: str, interval: str = "days", ni: int = 7):
    global jfreport

    set_job_controller_deamon(project_name=project_name)
    jfreport = JobsReport().generate_report(
        job_controller, interval=interval, num_intervals=ni
    )

def get_runner_status(proj_name: str) -> Tuple[str, str]:
    dm = daemon_managers[proj_name]
    current_status = dm.check_status()
    color = status_colors[current_status]

    return current_status.name, color

def serialize_mongo_document(data_item: Any) -> Any:
    if isinstance(data_item, ObjectId):
        return str(data_item)
    if isinstance(data_item, datetime):
        return data_item.isoformat()
    if isinstance(data_item, dict):
        return {key: serialize_mongo_document(value) for key, value in data_item.items()}
    if isinstance(data_item, list):
        return [serialize_mongo_document(element) for element in data_item]
    return data_item

start_stop_btn_lbl = {
    "RUNNING": "Stop",
    "SHUT_DOWN": "Start",
    "STOPPED": "Start",
}

status_colors = {
    DaemonStatus.STOPPED: "red",
    DaemonStatus.STOPPING: "orange",
    DaemonStatus.SHUT_DOWN: "red",
    DaemonStatus.PARTIALLY_RUNNING: "lawngreen",
    DaemonStatus.STARTING: "aqua",
    DaemonStatus.RUNNING: "limegreen",
}

@router.get("/", response_class=JSONResponse)
async def get_home(request: Request):
    cm = ConfigManager()
    list_projects = list(cm.projects.keys())
    return JSONResponse(content={"projects": list_projects})


@router.get("/projects", response_class=JSONResponse)
async def get_proj_home(request: Request, proj_name: str = ""):
    if proj_name not in list_projects:
        raise HTTPException(status_code=404, detail=f"Project {proj_name} not found")

    set_job_controller_deamon(proj_name)

    global jfreport

    if jfreport and hasattr(jfreport, 'trends'):
        interval = jfreport.trends.interval
        ni = jfreport.trends.num_intervals
    else:
        interval = "days"
        ni = 7

    return JSONResponse(content={"proj_name": proj_name, "interval": interval, "ni": ni})

@router.post("/actions/{action}/{what}/open_dialog", response_class=JSONResponse)
async def open_dialog(
    request: Request,
    action: ActionEnum,
    what: WhatEnum,
    selected: List[str] = Body(...),
):
    delete_options = False if (action != ActionEnum.delete or what != WhatEnum.flows) else True
    return JSONResponse(content={
        "action": action.value,
        "what": what.value,
        "selected": selected,
        "delete_options": delete_options,
        
    })

@router.post("/actions/{action}/{what}/run", response_model=ActionConfirmationResponse)
async def run_action(
    request: Request,
    action: ActionEnum,
    what: WhatEnum,
    selected: List[str] = Body(...),
    delete_output: bool = Body(False),
    delete_files: bool = Body(False),
    project_name: str = Body(...),
):
    set_job_controller_deamon(project_name)

    if what == WhatEnum.flows:
        response = job_controller_actions[what.value][action.value](
            selected, delete_output=delete_output, delete_files=delete_files
        )
    else:
        response = job_controller_actions[what.value][action.value](db_ids=selected)

    return ActionConfirmationResponse(
        action=action.value,
        what=what.value,
        response=response,
        delete_output=delete_output,
        delete_files=delete_files,
    )

@router.post("/runner/{proj_name}/{action}", response_model=RunnerStatusResponse)
async def runner_action(request: Request, proj_name: str, action: str):
    set_job_controller_deamon(proj_name)
    dm = daemon_managers[proj_name]
    if action == "start":
        dm.start()
        status = "STARTING"
    elif action == "stop":
        dm.shut_down()
        status = "STOPPING"
    else:
        raise HTTPException(status_code=400, detail="Invalid action")

    color = status_colors[DaemonStatus(status)]
    status_text = f"{status}"

    return RunnerStatusResponse(
        proj_name=proj_name,
        status=status_text,
        color=color,
        start_stop_btn_lbl=start_stop_btn_lbl,
    )

@router.get("/runner/{proj_name}/status", response_model=RunnerStatusResponse)
async def get_runner_status_update(request: Request, proj_name: str):
    status, color = get_runner_status(proj_name)
    return RunnerStatusResponse(
        proj_name=proj_name,
        status=status,
        color=color,
        start_stop_btn_lbl=start_stop_btn_lbl,
    )


@router.get("/{proj_name}/{what}/sum_report", response_model=SumReportResponse)
async def sum_report(request: Request, proj_name: str, what: WhatEnum):
    update_jfreport(project_name=proj_name)
    global jfreport
    state_counts = Counter(jfreport.state_counts)

    most_common_state, most_common_count = state_counts.most_common(1)[0]

    running_count = jfreport.running
    completed_count = jfreport.completed
    error_count = jfreport.error
    active_count = jfreport.active

    total_jobs = state_counts.total()
    state_percentages = {
        state: (count / total_jobs) * 100 for state, count in state_counts.items()
    }

    return SumReportResponse(
        what=what.value,
        total_jobs=total_jobs,
        most_common_state=most_common_state.name,
        most_common_count=most_common_count,
        most_common_percentage=f"{state_percentages[most_common_state]:.2f}",
        running_count=running_count,
        completed_count=completed_count,
        error_count=error_count,
        active_count=active_count,
        longest_running=jfreport.longest_running,
        worker_utilization=jfreport.worker_utilization,
    )

@router.post("/{proj_name}/{what}/trends/", response_model=TrendsResponse)
async def trends(
    request: Request,
    proj_name: str,
    what: WhatEnum,
    payload: dict = Body(...)
):
    interval = payload.get("interval", "days")
    ni = payload.get("ni", 7)

    update_jfreport(project_name=proj_name, interval=interval, ni=ni)

    global jfreport

    trend_data = TrendData(
        dates=jfreport.trends.dates,
        completed=jfreport.trends.completed,
        failed=jfreport.trends.failed,
        remote_error=jfreport.trends.remote_error
    )

    return TrendsResponse(what=what.value, trend_data=trend_data)

@router.get("/{proj_name}/{what}/state_distro", response_model=StateDistroResponse)
async def state_distro(request: Request, proj_name: str, what: WhatEnum):
    update_jfreport(project_name=proj_name)

    global jfreport
    state_counts = Counter(jfreport.state_counts)

    total_jobs = state_counts.total()
    state_percentages = {
        state: (count / total_jobs) * 100 for state, count in state_counts.items()
    }

    state_data = [
        StateData(state=state.name, count=count, percentage=f"{state_percentages[state]:.2f}")
        for state, count in state_counts.items()
    ]

    return StateDistroResponse(what=what.value, state_data=state_data)



@router.get("/{proj_name}/{what}/info/{jf_id}", response_model=JobFlowInfoResponse)
async def get_info_job_flow(request: Request, jf_id: str, what: WhatEnum, proj_name: str):
    
    set_job_controller_deamon(project_name=proj_name)
    processed_info = None

    if what == WhatEnum.jobs:
        job_data = job_controller.get_job_info(db_id=jf_id)
        if job_data:
            processed_info = job_data.dict()
    elif what == WhatEnum.flows:
        raw_flow_info = job_controller.get_flow_info_by_flow_uuid(jf_id)
        if raw_flow_info:
            processed_info = serialize_mongo_document(raw_flow_info)


    if processed_info:
        return JobFlowInfoResponse(jf_id=jf_id, what=what.value, info=processed_info)
    raise HTTPException(status_code=404, detail="Job/Flow not found")


@router.get("/{proj_name}/flows/graph/{jf_id}")
async def get_graph_job_flow(request: Request, jf_id: str, proj_name: str):
    set_job_controller_deamon(project_name=proj_name)
    flowinfo = job_controller.get_flows_info(limit=1, full=True)[0]
    graph = get_mermaid(flowinfo)

    return JSONResponse(content={"graph": graph})



@router.get("/{proj_name}/{what}/query")
async def get_query_page(request: Request, proj_name: str, what: WhatEnum):
    set_job_controller_deamon(project_name=proj_name)
    if what == "jobs":
        total_entries = job_controller.count_jobs()
    elif what == "flows":
        total_entries = job_controller.count_flows()
    else:
        total_entries = 0

    return JSONResponse(content={
            "proj_name": proj_name,
            "what": what.value,
            "total_entries": total_entries,
            "job_states": [state.value for state in JobState],
        })


@router.post("/{proj_name}/{what}/query", response_model=QueryResponse)
async def post_query(
    request: Request,
    proj_name: str,
    what: WhatEnum,
    body: dict = Body(...),
):
    set_job_controller_deamon(project_name=proj_name)
    
    query: Dict[str, Any] = {}

    db_id = body.get('db_id')
    uuid = body.get('uuid')
    name = body.get('name')
    state = body.get('state')
    worker = body.get('worker')
    start_date_str = body.get('start_date')
    start_time_str = body.get('start_time')
    end_date_str = body.get('end_date')
    end_time_str = body.get('end_time')
    timezone_in_str = body.get('timezone_in')
    sort_by = body.get('sort_by', "updated_on")
    sort_order = body.get('sort_order', -1)
    page = body.get('page', 1)
    entries_per_page = body.get('entries_per_page', 10)

    if db_id:
        query["db_ids"] = [db_id]
    if uuid:
        query["job_ids"] = [(uuid, None)]
    if name:
        query["name"] = name
    if state:
        query["states"] = None if state == "Any" else JobState(state)
    if worker:
        query["metadata"] = {"worker": worker}

    if timezone_in_str:
        tz = ZoneInfo(timezone_in_str)
    else:
        tz = ZoneInfo("UTC")

    if start_time_str and not start_date_str:
        start_date_str = datetime.now().strftime("%Y-%m-%d")
    if start_date_str:
        if start_time_str:
            start_datetime_str = f"{start_date_str} {start_time_str}"
            query["start_date"] = datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M").replace(tzinfo=tz)
        else:
            query["start_date"] = datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=tz)

    if end_time_str and not end_date_str:
        end_date_str = datetime.now().strftime("%Y-%m-%d")
    if end_date_str:
        if end_time_str:
            end_datetime_str = f"{end_date_str} {end_time_str}"
            query["end_date"] = datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M").replace(tzinfo=tz)
        else:
            query["end_date"] = datetime.strptime(end_date_str, "%Y-%m-%d").replace(tzinfo=tz)

    skip = (page - 1) * entries_per_page

    jobs_flows_data = []
    if what == WhatEnum.jobs:
        jobs_flows_data = job_controller.get_jobs_info(
            **query, sort=[[sort_by, sort_order]], limit=entries_per_page, skip=skip
        )
    elif what == WhatEnum.flows:
        jobs_flows_data = job_controller.get_flows_info(
            **query, sort=[[sort_by, sort_order]], limit=entries_per_page, skip=skip
        )

    if not jobs_flows_data:
         # Return an empty list or a message, but ensure QueryResponse structure if possible
        return QueryResponse(
            proj_name=proj_name,
            what=what.value,
            all_entries=[],
            sort_by=sort_by,
            next_sort_order=-sort_order,
            action_btns_lbl=(),
            page=page,
            total_pages=0,
            entries_per_page=entries_per_page,
            db_id=db_id,
            uuid=uuid,
            name=name,
            state=state,
            worker=worker,
            start_date=start_date_str,
            start_time=start_time_str,
            end_date=end_date_str,
            end_time=end_time_str,
            timezone_in=timezone_in_str,
            total_entries=0
        )


    next_sort_order = -sort_order
    all_entries = [serialize_mongo_document(entry.dict()) for entry in jobs_flows_data]

    action_btns_lbl_tuple: Tuple[str, ...] = ()
    if what == WhatEnum.jobs:
        action_btns_lbl_tuple = ("Rerun", "Play", "Pause", "Stop", "Retry")
    elif what == WhatEnum.flows:
        action_btns_lbl_tuple = ("Delete",)

    total_entries_count = 0
    if what == "jobs":
        total_entries_count = job_controller.count_jobs(**query)
    elif what == "flows":
        total_entries_count = job_controller.count_flows(**query)

    total_pages_val = ceil(total_entries_count / entries_per_page) if entries_per_page > 0 else 0


    return QueryResponse(
        proj_name=proj_name,
        what=what.value,
        all_entries=all_entries,
        sort_by=sort_by,
        next_sort_order=next_sort_order,
        action_btns_lbl=action_btns_lbl_tuple,
        page=page,
        total_pages=total_pages_val,
        entries_per_page=entries_per_page,
        db_id=db_id,
        uuid=uuid,
        name=name,
        state=state,
        worker=worker,
        start_date=start_date_str,
        start_time=start_time_str,
        end_date=end_date_str,
        end_time=end_time_str,
        timezone_in=timezone_in_str,
        total_entries=total_entries_count,
    )