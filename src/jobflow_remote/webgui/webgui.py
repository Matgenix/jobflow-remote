from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from math import ceil
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from monty.dev import requires

try:
    import fasthtml
    from fasthtml.common import (
        H1,
        H3,
        H4,
        A,
        Button,
        CheckboxX,
        Details,
        Dialog,
        Div,
        Favicon,
        Form,
        Img,
        Input,
        Label,
        Li,
        Link,
        Main,
        Option,
        P,
        Script,
        Select,
        Span,
        Summary,
        Table,
        Td,
        Th,
        Title,
        Tr,
        Ul,
        fast_app,
        serve,
    )
    from fasthtml.pico import Card, Group
except ImportError:
    fasthtml = None

    # fake rt decorator
    def rt_fake(*args, **kwargs):
        def wrapper(func):
            return func

        return wrapper

    def fast_app(*args, **kwargs):
        return lambda x: x, rt_fake

    def fake_function(*args, **kwargs):
        return None

    Title = Link = Script = Favicon = fake_function


from jobflow_remote import ConfigManager
from jobflow_remote.jobs.daemon import DaemonManager, DaemonStatus
from jobflow_remote.jobs.graph import get_mermaid
from jobflow_remote.jobs.jobcontroller import JobController
from jobflow_remote.jobs.report import FlowsReport, JobsReport
from jobflow_remote.jobs.state import FlowState, JobState
from jobflow_remote.webgui.palette import state_color

# Bokeh is part of the optional ``gui`` extra. Import the chart helpers and the
# versioned BokehJS CDN URLs together: if Bokeh is missing, charts are simply
# skipped and the existing tables remain.
try:
    from bokeh.resources import CDN

    from jobflow_remote.webgui import charts

    bokeh_headers = tuple(Script(src=url) for url in CDN.js_files)
except ImportError:
    charts = None
    bokeh_headers = ()

id_curr = "current-info"
id_list = "info-list"


PAGE_TITLE = Title("Jobflow remote manager")

mermaid_js = """
import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
mermaid.initialize({ startOnLoad: false });
window.mermaid = mermaid;
"""


def error_handler(req, exc):
    error_message = f"{exc}"
    return Dialog(
        Div(
            H3("An error occurred:"),
            P(f"{error_message}"),
            Button(
                "Cancel",
                hx_get="/test/close_dialog",
                # Remove the dialog itself: it may be injected on a page that has
                # no #dialog-container, so target the dialog by its own id.
                hx_target="#my-dialog",
                hx_swap="outerHTML",
                style="font-weight: bold",
            ),
            cls="card-dialog",
        ),
        id="my-dialog",
        open="open",  # This attribute opens the dialog
        cls="dialog",
    )


exception_handlers = {500: error_handler}

app, rt = fast_app(
    pico=False,  # Disable Pico CSS to use custom styles
    hdrs=(
        Link(rel="stylesheet", href="/style.css", type="text/css"),
        Script(mermaid_js, type="module"),
        *bokeh_headers,
        Favicon("/jfr_favicon.ico", "/jfr_favicon.ico"),
    ),  # Add custom CSS as a header
    exception_handlers=exception_handlers,
    static_path=Path(__file__).parent,
)


# javascript
js_timezone = """
document.getElementById('timezone_in').value = Intl.DateTimeFormat().resolvedOptions().timeZone;
"""

# change color on click
js_set_color = """
  function setActiveLink(clickedLink) {
    // Remove active class from all links
    document.querySelectorAll('.navbar-link').forEach(link => {
      link.classList.remove('active');
    });

    // Add active class to clicked link
    clickedLink.classList.add('active');
  }
"""

start_stop_btn_lbl = {
    "RUNNING": "Stop",
    "SHUT_DOWN": "Start",
    "STOPPED": "Start",
}

status_colors = {
    DaemonStatus.STOPPED: "red",
    DaemonStatus.STOPPING: "aqua",
    DaemonStatus.SHUT_DOWN: "red",
    DaemonStatus.PARTIALLY_RUNNING: "lawngreen",
    DaemonStatus.STARTING: "aqua",
    DaemonStatus.RUNNING: "limegreen",
}

cm = ConfigManager()

list_projects = list(cm.projects.keys())

# Per-project caches keyed by project name. Resources are created lazily on
# first access so that routes work even after a server reload or when a deep
# URL is hit directly (no implicit "current project" state to initialize).
job_controllers: dict[str, JobController] = {}
daemon_managers: dict[str, DaemonManager] = {}
# Reports are cached per (project name, "jobs"|"flows") and only refreshed
# explicitly (via the "Update" buttons or a changed trend interval).
reports: dict[tuple[str, str], JobsReport | FlowsReport] = {}


def get_job_controller(proj_name: str) -> JobController:
    """Return the cached ``JobController`` for the project, creating it if needed."""
    if proj_name not in job_controllers:
        job_controllers[proj_name] = JobController.from_project_name(
            project_name=proj_name
        )
    return job_controllers[proj_name]


def get_daemon_manager(proj_name: str) -> DaemonManager:
    """Return the cached ``DaemonManager`` for the project, creating it if needed."""
    if proj_name not in daemon_managers:
        daemon_managers[proj_name] = DaemonManager.from_project(
            cm.get_project(proj_name)
        )
    return daemon_managers[proj_name]


def get_job_controller_actions(proj_name: str) -> dict:
    """Return the mapping of action labels to ``JobController`` methods."""
    jc = get_job_controller(proj_name)
    return {
        "jobs": {
            "Resume": jc.resume_jobs,
            "Pause": jc.pause_jobs,
            "Stop": jc.stop_jobs,
            "Retry": jc.retry_jobs,
            "Rerun": jc.rerun_jobs,
        },
        "flows": {
            "Delete": jc.delete_flows,
        },
    }


def update_report(
    proj_name: str, what: str = "jobs", interval: str = "days", ni: int = 7
) -> JobsReport | FlowsReport:
    """Regenerate and cache the report for the given project and type."""
    jc = get_job_controller(proj_name)
    report_cls = FlowsReport if what == "flows" else JobsReport
    report = report_cls.generate_report(jc, interval=interval, num_intervals=ni)
    reports[(proj_name, what)] = report
    return report


def get_report(proj_name: str, what: str = "jobs") -> JobsReport | FlowsReport | None:
    """Return the cached report for the project, or ``None`` if not generated yet."""
    return reports.get((proj_name, what))


# States that require user attention, shown in the dashboard attention list.
ATTENTION_STATES = [JobState.FAILED, JobState.REMOTE_ERROR, JobState.PAUSED]


def state_badge(state: JobState | FlowState):
    """
    Render a job/flow state as a small colored badge.

    The badge color comes from the shared :data:`~jobflow_remote.webgui.palette`
    map, so tables, the dashboard attention list and the charts all use the same
    visual language for a given state.

    Parameters
    ----------
    state
        The ``JobState`` or ``FlowState`` to render.

    Returns
    -------
    FT
        A ``Span`` element styled as a state badge.
    """
    return Span(
        state.name, cls="state-badge", style=f"background:{state_color(state.name)};"
    )


def state_overview(state_counts: dict, what: str):
    """
    Build the "completed" headline metric and the category bar chart.

    Parameters
    ----------
    state_counts
        Mapping of state to count for the jobs/flows.
    what
        Either ``"jobs"`` or ``"flows"`` (used only for empty messages).

    Returns
    -------
    FT | None
        A component with the progress headline and the category bar chart, or
        ``None`` when Bokeh is unavailable.
    """
    if charts is None:
        return None
    completed, total, categories = charts.categorize_state_counts(
        dict(state_counts), what
    )
    if total == 0:
        return P(f"No {what} found.")
    pct = (completed / total * 100) if total else 0.0
    headline = Div(
        Span(f"Completed: {completed} / {total} ", cls="progress-num"),
        Span(f"({pct:.1f}%)", cls="progress-pct"),
        Div(
            Div(style=f"width: {pct:.1f}%;", cls="progress-fill"),
            cls="progress-track",
        ),
        cls="progress-headline",
    )
    return Div(
        headline, charts.bokeh_chart(charts.category_bar_figure(categories, what))
    )


def _format_elapsed(now: datetime, start: datetime | None) -> str:
    """Return a compact ``Hh Mm`` / ``Mm Ss`` string for ``now - start``."""
    if start is None:
        return "-"
    total = max(0, int((now - start.replace(tzinfo=timezone.utc)).total_seconds()))
    hours, remainder = divmod(total, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def dashboard_health_strip(proj_name: str, jobs_report: JobsReport):
    """Build the runner-status + attention-counts strip at the top of the dashboard."""
    status, color = get_runner_status(proj_name)
    state_counts = jobs_report.state_counts
    badge_specs = [
        ("FAILED", JobState.FAILED, "#e74c3c"),
        ("REMOTE_ERROR", JobState.REMOTE_ERROR, "#e67e22"),
        ("PAUSED", JobState.PAUSED, "#9b59b6"),
    ]
    badges = [
        Span(
            f"{state_counts.get(st, 0)} {label}",
            cls="health-badge",
            style=f"background:{c};",
        )
        for label, st, c in badge_specs
        if state_counts.get(st, 0)
    ]
    if not badges:
        badges = [Span("All clear", cls="health-badge", style="background:#2ecc71;")]
    return Div(
        Span("Runner:", style="font-weight:bold;"),
        Span(status, cls="runner-pill", style=f"color:{color};"),
        Span("Attention:", style="font-weight:bold; margin-left:16px;"),
        *badges,
        cls="health-strip",
    )


def dashboard_kpis(jobs_report: JobsReport, flows_report: FlowsReport):
    """Build the row of headline KPI cards."""
    flows_total = sum(flows_report.state_counts.values())
    jobs_total = sum(jobs_report.state_counts.values())
    failure_rate = (jobs_report.error / jobs_total * 100) if jobs_total else 0.0
    cards = [
        ("Flows", str(flows_total)),
        ("Flows running", str(flows_report.running)),
        ("Flows completed", str(flows_report.completed)),
        ("Job failure rate", f"{failure_rate:.1f}%"),
    ]
    return Div(
        *[
            Div(
                Div(value, cls="kpi-value"),
                Div(label, cls="kpi-label"),
                cls="kpi-card",
            )
            for label, value in cards
        ],
        cls="kpi-row",
    )


def dashboard_worker_card(jobs_report: JobsReport):
    """Build the worker-utilization card."""
    util = jobs_report.worker_utilization
    if charts is None or not util:
        return Div(H4("Worker utilization"), P("No data."), cls="card")
    return Div(
        H4("Worker utilization"),
        charts.bokeh_chart(charts.worker_utilization_figure(util)),
        cls="card",
    )


def dashboard_longest_running(jobs_report: JobsReport, proj_name: str):
    """Build the longest-running-jobs card."""
    now = datetime.now(timezone.utc)
    jobs = jobs_report.longest_running
    if not jobs:
        return Div(H4("Longest running jobs"), P("No running jobs."), cls="card")
    rows = [
        Tr(
            Td(
                A(
                    job.db_id,
                    hx_get=f"/{proj_name}/jobs/dialog/{job.db_id}",
                    hx_target="#dialog-container",
                    hx_swap="innerHTML",
                )
            ),
            Td(job.name),
            Td(job.worker),
            Td(_format_elapsed(now, job.start_time)),
        )
        for job in jobs
    ]
    return Div(
        H4("Longest running jobs"),
        Table(Tr(Th("DB id"), Th("Name"), Th("Worker"), Th("Elapsed")), *rows),
        cls="card",
    )


def dashboard_attention_list(proj_name: str):
    """Build the table of jobs that need attention (failed / remote-error / paused)."""
    job_controller = get_job_controller(proj_name)
    jobs = job_controller.get_jobs_info(
        states=ATTENTION_STATES, sort=[["updated_on", -1]], limit=20
    )
    if not jobs:
        return Div(
            H4("Needs attention"),
            P("No jobs need attention."),
            cls="card",
        )
    rows = [
        Tr(
            Td(
                A(
                    job.db_id,
                    hx_get=f"/{proj_name}/jobs/dialog/{job.db_id}",
                    hx_target="#dialog-container",
                    hx_swap="innerHTML",
                )
            ),
            Td(job.name),
            Td(state_badge(job.state)),
            Td(job.worker),
            Td(job.updated_on.replace(tzinfo=timezone.utc).strftime("%Y-%m-%d %H:%M")),
        )
        for job in jobs
    ]
    return Div(
        H4("Needs attention (failed / remote-error / paused)"),
        Table(
            Tr(Th("DB id"), Th("Name"), Th("State"), Th("Worker"), Th("Updated")),
            *rows,
        ),
        cls="card",
    )


def dashboard_trends_chart(jobs_report: JobsReport):
    """Build the jobs throughput chart, titled to make clear it refers to jobs."""
    if charts is None:
        return None
    return charts.bokeh_chart(
        charts.trends_figure(jobs_report.trends, title="Throughput over time (jobs)")
    )


def dashboard_trends_card(proj_name: str, jobs_report: JobsReport):
    """
    Build the throughput card with an interval selector.

    The selector lets the user change the time range of the jobs throughput
    chart (as the standalone trends view did), refreshing only the chart.
    """
    interval = jobs_report.trends.interval
    ni = jobs_report.trends.num_intervals
    return Div(
        H4("Throughput over time (jobs)"),
        Form(
            Label("Last"),
            Input(type="number", value=ni, name="ni", style="width: 70px;"),
            Select(
                *[
                    Option(i, selected=i == interval)
                    for i in ("days", "hours", "months", "years")
                ],
                name="interval",
            ),
            Button(
                "Update",
                hx_post=f"/{proj_name}/dashboard/trends",
                hx_include="[name='interval'],[name='ni']",
                hx_target="#dashboard-trends-chart",
                hx_swap="innerHTML",
            ),
            cls="group",
        ),
        Div(dashboard_trends_chart(jobs_report), id="dashboard-trends-chart"),
        cls="card",
    )


@rt("/{proj_name}/dashboard/trends", methods=["POST"])
def get_dashboard_trends(proj_name: str, interval: str = "days", ni: int = 7):
    """Regenerate the jobs report if needed and return the updated throughput chart."""
    report = get_report(proj_name, "jobs")
    if (
        not report
        or report.trends.interval != interval
        or report.trends.num_intervals != ni
    ):
        report = update_report(proj_name, "jobs", interval, ni)
    return dashboard_trends_chart(report)


def build_dashboard(proj_name: str):
    """
    Assemble the full project dashboard.

    Reports are taken from the cache when available and generated on demand
    otherwise, so the dashboard is cheap to re-render but refreshes its data only
    when explicitly asked to (the "Refresh" button regenerates the reports).
    """
    jobs_report = get_report(proj_name, "jobs") or update_report(proj_name, "jobs")
    flows_report = get_report(proj_name, "flows") or update_report(proj_name, "flows")

    return Div(
        Div(
            H3(f"Dashboard: {proj_name}"),
            Button(
                "Refresh",
                hx_get=f"/{proj_name}/dashboard",
                hx_target="#dashboard",
                hx_swap="outerHTML",
            ),
            cls="dashboard-header",
        ),
        dashboard_health_strip(proj_name, jobs_report),
        dashboard_kpis(jobs_report, flows_report),
        Div(
            Div(
                H4("Flows"),
                state_overview(flows_report.state_counts, "flows"),
                cls="card",
            ),
            Div(
                H4("Jobs"), state_overview(jobs_report.state_counts, "jobs"), cls="card"
            ),
            cls="dashboard-two-col",
        ),
        dashboard_trends_card(proj_name, jobs_report),
        Div(
            dashboard_worker_card(jobs_report),
            dashboard_longest_running(jobs_report, proj_name),
            cls="dashboard-two-col",
        ),
        dashboard_attention_list(proj_name),
        id="dashboard",
    )


@rt("/{proj_name}/dashboard")
def get_dashboard(proj_name: str):
    """Regenerate the reports and return the refreshed dashboard fragment."""
    update_report(proj_name, "jobs")
    update_report(proj_name, "flows")
    return build_dashboard(proj_name)


# Navigation bar component
def projectbar(proj_name: str = "", what: str = ""):
    if proj_name:
        status, color = get_runner_status(proj_name)
    return Div(
        Script(js_set_color),
        Ul(
            A(Img(src="/logo_jfr.png", height=50), href="/"),
            Li("Projects:"),
            Li(
                Select(
                    Option("Pick one"),
                    *[
                        Option(prj, value=prj, selected=(proj_name == prj))
                        for prj in list_projects
                    ],
                    name="proj_name",
                    hx_push_url="true",
                    hx_get="/projects",
                    hx_target="body",
                )
            ),
            (
                Li(
                    A(
                        "Dashboard",
                        hx_get=f"/projects?proj_name={proj_name}",
                        hx_push_url="true",
                        hx_target="body",
                        # _="on click remove .active from .navbar a then add .active to me",
                        onclick="setActiveLink(this)",
                        cls="navbar-link",
                    )
                ),
                Group(
                    Ul(
                        Li("Query: "),
                        Li(
                            A(
                                "Jobs",
                                hx_get=f"/{proj_name}/jobs/query",
                                hx_target="#prj-container",
                                hx_push_url="true",
                                # _="on click remove .active from .navbar a then add .active to me",
                                onclick="setActiveLink(this)",
                                cls="navbar-link",
                            )
                        ),
                        Li(
                            A(
                                "Flows",
                                hx_get=f"/{proj_name}/flows/query",
                                hx_target="#prj-container",
                                hx_push_url="true",
                                # _="on click remove .active from .navbar a then add .active to me",
                                onclick="setActiveLink(this)",
                                cls="navbar-link",
                            )
                        ),
                    ),
                    cls="group",
                ),
                Group(
                    Ul(
                        Li("Runner:"),
                        Li(f"{status}", cls="runner-pill", style=f"color: {color};"),
                        Li(
                            Button(
                                f"{start_stop_btn_lbl[status]}",
                                hx_post=f"/runner/{proj_name}/{start_stop_btn_lbl[status].lower()}",
                                hx_target="#runner-status",
                            )
                        ),
                    ),
                    hx_get=f"/runner/{proj_name}/status",
                    hx_trigger="every 30s",
                    hx_swap="outerHTML",
                    id="runner-status",
                    cls="group",
                ),
            )
            if proj_name
            else None,
        ),
        cls="navbar",
    )


def ScrollableArea(*content, height: str = "300px"):
    """Wrap content in a fixed-height, vertically scrollable box."""
    return Div(*content, cls="scrollable-area", style=f"height: {height};")


@rt("/")
def get_home():
    return PAGE_TITLE, Main(
        projectbar(),
        Div(
            H3("Jobflow Remote manager"),
            P(
                "Select a project from the navigation bar above to view its "
                "dashboard and query its jobs and flows."
            ),
            P(
                f"Available projects: {', '.join(list_projects)}."
                if list_projects
                else "No projects are currently configured.",
                cls="muted",
            ),
            cls="container",
            id="prj-container",
        ),
        id="page",
    )


@rt("/actions/{proj_name}/{action}/{what}/open_dialog", methods=["POST"])
def open_dialog(proj_name: str, action: str, what: str, kwargs: dict):
    selected = [f"{v}" for k, v in kwargs.items()]

    # Add checkboxes for delete_flow options
    delete_options = ""
    if action == "Delete" and what == "flows":
        delete_options = Div(
            CheckboxX(
                id="delete_output",
                name="delete_options",
                value="delete_output",
                label="Delete Output",
            ),
            CheckboxX(
                id="delete_files",
                name="delete_options",
                value="delete_files",
                label="Delete Files",
            ),
        )

    return Dialog(
        Div(
            H3(f"Action Selected: {action}"),
            P(f"{what} selected:"),
            P(", ".join(selected) if selected else "No options selected"),
            P("Options:"),
            delete_options,
            Button(
                "Cancel",
                hx_get="/test/close_dialog",
                hx_target="#dialog-container",
                style="",
            ),
            Button(
                "Confirm",
                hx_post=f"/actions/{proj_name}/{action}/{what}/run",
                hx_include="[name='ckbx_action'],[name='delete_options']",
                hx_target="#dialog-container",
                hx_swap="innerHTML",
                style="font-weight: bold",
            )
            if selected
            else None,
            cls="card-dialog",
        ),
        id="my-dialog",
        open="open",  # This attribute opens the dialog
        cls="dialog",
    )


@rt("/actions/{proj_name}/{action}/{what}/run", methods=["POST"])
def run_action(proj_name: str, action: str, what: str, kwargs: dict):
    selected = [v for k, v in kwargs.items() if k != "delete_options"]
    delete_options = kwargs.get("delete_options", [])

    actions = get_job_controller_actions(proj_name)
    if action == "Delete" and what == "flows":
        delete_output = "delete_output" in delete_options
        delete_files = "delete_files" in delete_options
        response = actions[what][action](
            selected, delete_output=delete_output, delete_files=delete_files
        )
    else:
        response = actions[what][action](db_ids=selected)

    return Dialog(
        Div(
            H3(f"Action Selected: {action}"),
            P(f"Applied on {response} {what}"),
            P(f"{delete_options}"),
            Button(
                "Cancel",
                hx_get="/test/close_dialog",
                hx_target="#dialog-container",
                style="font-weight: bold",
            ),
            cls="card-dialog",
        ),
        id="my-dialog",
        open="open",  # This attribute opens the dialog
        cls="dialog",
    )


@rt("/test/close_dialog")
def close_dialog():
    return ""


# handle starting and stopping the runner
@rt("/runner/{proj_name}/start", methods=["POST"])
def start_runner_route(proj_name: str):
    dm = get_daemon_manager(proj_name)
    dm.start()
    status = "STARTING"
    color = status_colors[DaemonStatus(status)]
    return Group(
        Ul(
            Li("Runner:"),
            Li(status, cls="runner-pill", style=f"color: {color};"),
        ),
        id="runner-status",
        hx_get=f"/runner/{proj_name}/status",
        hx_trigger="every 5s",
        hx_swap="outerHTML",
        cls="group",
    )


@rt("/runner/{proj_name}/stop", methods=["POST"])
def stop_runner_route(proj_name: str):
    dm = get_daemon_manager(proj_name)
    dm.shut_down()
    status = "STOPPING"
    color = status_colors[DaemonStatus(status)]
    return Group(
        Ul(
            Li("Runner:"),
            Li(status, cls="runner-pill", style=f"color: {color};"),
        ),
        id="runner-status",
        hx_get=f"/runner/{proj_name}/status",
        hx_trigger="every 5s",
        hx_swap="outerHTML",
        cls="group",
    )


@rt("/runner/{proj_name}/status")
def get_runner_status_update(proj_name: str):
    status, color = get_runner_status(proj_name)
    return Group(
        Ul(
            Li("Runner:"),
            Li(f"{status}", cls="runner-pill", style=f"color: {color};"),
            Li(
                Button(
                    f"{start_stop_btn_lbl[status]}",
                    hx_post=f"/runner/{proj_name}/{start_stop_btn_lbl[status].lower()}",
                    hx_target="#runner-status",
                )
            ),
        ),
        hx_get=f"/runner/{proj_name}/status",
        hx_trigger="every 30s",
        hx_swap="outerHTML",
        id="runner-status",
        cls="group",
    )


def get_runner_status(proj_name: str):
    dm = get_daemon_manager(proj_name)
    current_status = dm.check_status()
    color = status_colors[current_status]

    return current_status.name, color


@rt("/projects")
def get_proj_home(proj_name: str = ""):
    if proj_name not in list_projects:
        return PAGE_TITLE, Main(
            projectbar(),
            Div(
                H1(f"The project: {proj_name} does not exists"),
                P("Select an available project from the navigation bar above."),
                id="prj-container",
            ),
        )

    jobs_report = get_report(proj_name, "jobs")
    if jobs_report:
        interval = jobs_report.trends.interval
        ni = jobs_report.trends.num_intervals
    else:
        interval = "days"
        ni = 7

    return PAGE_TITLE, Main(
        Script(mermaid_js, type="module"),
        projectbar(proj_name),
        Div(
            build_dashboard(proj_name),
            Details(
                Summary("Detailed reports"),
                Div(
                    H3("Jobs Report"),
                    Ul(
                        Li(
                            Button(
                                "Summary Jobs Report",
                                hx_get=f"/{proj_name}/jobs/sum_report",
                                hx_target="#jobs-report",
                            )
                        ),
                        Li(
                            Button(
                                "Jobs State Distribution",
                                hx_get=f"/{proj_name}/jobs/state_distro",
                                hx_target="#jobs-report",
                            )
                        ),
                        Li(
                            Form(
                                Button(
                                    "Jobs Trend for the last",
                                    hx_post=f"/{proj_name}/jobs/trends/",
                                    hx_include="[name='interval'],[name='ni']",
                                    hx_target="#jobs-report",
                                ),
                                Select(
                                    *[
                                        Option(i, selected=i == interval)
                                        for i in ("days", "hours", "months", "years")
                                    ],
                                    name="interval",
                                ),
                                Input(type="number", value=ni, name="ni"),
                            )
                        ),
                    ),
                    Div(id="jobs-report"),
                    id="buttons-jobs-report",
                    cls="card",
                ),
                Div(
                    H3("Flows Report"),
                    Ul(
                        Li(
                            Button(
                                "Summary Flows Report",
                                hx_get=f"/{proj_name}/flows/sum_report",
                                hx_target="#flows-report",
                            )
                        ),
                        Li(
                            Button(
                                "Flows State Distribution",
                                hx_get=f"/{proj_name}/flows/state_distro",
                                hx_target="#flows-report",
                            )
                        ),
                        Li(
                            Form(
                                Button(
                                    "Flows Trend for the last",
                                    hx_post=f"/{proj_name}/flows/trends/",
                                    hx_include="[name='interval'],[name='ni']",
                                    hx_target="#flows-report",
                                ),
                                Select(
                                    *[
                                        Option(i, selected=i == interval)
                                        for i in ("days", "hours", "months", "years")
                                    ],
                                    name="interval",
                                ),
                                Input(type="number", value=ni, name="ni"),
                            )
                        ),
                    ),
                    Div(id="flows-report"),
                    id="buttons-flows-report",
                    cls="card",
                ),
            ),
            Div(Script(mermaid_js, type="module"), id="dialog-container"),
            id="prj-container",
        ),
    )


@rt("/{proj_name}/{what}/sum_report")
def sum_report(proj_name: str, what: str):
    report_data = get_report(proj_name, what)
    if not report_data:
        report_data = update_report(proj_name, what)

    state_counts = Counter(report_data.state_counts)
    total_jobs = state_counts.total()

    update_btn = Button(
        "Update",
        hx_get=f"/{proj_name}/{what}/sum_report",
        hx_target=f"#{what}-report",
    )

    if total_jobs == 0:
        return Div(
            Div(update_btn, P(f"No {what} found."), cls="job-state-report"),
            cls="container",
        )

    # Find the most common state
    most_common_state, most_common_count = state_counts.most_common(1)[0]

    running_count = report_data.running
    completed_count = report_data.completed
    error_count = report_data.error

    # Calculate percentages
    state_percentages = {
        state: (count / total_jobs) * 100 for state, count in state_counts.items()
    }

    rows = [
        Tr(Td(f"Total number of {what}:"), Td(f"{total_jobs}")),
        Tr(
            Td("Most common state:"),
            Td(
                f"{most_common_state.name} ({most_common_count} {what}, {state_percentages[most_common_state]:.2f}%)"
            ),
        ),
        Tr(Td(f"Running {what}:"), Td(f"{running_count}")),
        Tr(Td(f"Completed {what}:"), Td(f"{completed_count}")),
        Tr(
            Td(f"Sum of failed and remote error {what}:"),
            Td(f"{error_count}"),
        ),
    ]
    # The following metrics are only available in the jobs report.
    if what == "jobs":
        rows.extend(
            [
                Tr(Td(f"Sum of all active {what}:"), Td(f"{report_data.active}")),
                Tr(Td("Longest running:"), Td(f"{report_data.longest_running}")),
                Tr(Td("Worker utilization:"), Td(f"{report_data.worker_utilization}")),
            ]
        )

    # Create the report
    report = Div(
        update_btn,
        Table(*rows),
        cls="job-state-report",
    )

    return Div(report, cls="container")


@rt("/{proj_name}/{what}/trends/", methods=["POST"])
def trends(proj_name: str, what: str, interval: str = "days", ni: int = 7):
    report_data = get_report(proj_name, what)
    if (
        not report_data
        or report_data.trends.interval != interval
        or report_data.trends.num_intervals != ni
    ):
        report_data = update_report(proj_name, what, interval, ni)

    # trends table. Only the jobs report tracks the remote error trend.
    if what == "flows":
        trend_table = Table(
            Tr(Th("Dates"), Th("Completed"), Th("Failed")),
            *[
                Tr(Td(d), Td(c), Td(f))
                for d, c, f in zip(
                    report_data.trends.dates,
                    report_data.trends.completed,
                    report_data.trends.failed,
                    strict=True,
                )
            ],
            cls="trend-table",
        )
    else:
        trend_table = Table(
            Tr(Th("Dates"), Th("Completed"), Th("Failed"), Th("Remote error")),
            *[
                Tr(Td(d), Td(c), Td(f), Td(r))
                for d, c, f, r in zip(
                    report_data.trends.dates,
                    report_data.trends.completed,
                    report_data.trends.failed,
                    report_data.trends.remote_error,
                    strict=True,
                )
            ],
            cls="trend-table",
        )

    # Stacked bar chart of the trends (skipped if Bokeh is unavailable).
    chart = (
        charts.bokeh_chart(
            charts.trends_figure(
                report_data.trends, title=f"Throughput over time ({what})"
            )
        )
        if charts is not None
        else None
    )

    report = Div(
        chart,
        H3(f"{what.capitalize()} Trend Table:"),
        trend_table,
        cls="job-state-report",
    )

    return Div(report, cls="container")


@rt("/{proj_name}/{what}/state_distro")
def state_distro(proj_name: str, what: str):
    report_data = get_report(proj_name, what)
    if not report_data:
        report_data = update_report(proj_name, what)

    state_counts = Counter(report_data.state_counts)

    # Calculate percentages
    total_jobs = state_counts.total()
    update_btn = Button(
        "Update",
        hx_get=f"/{proj_name}/{what}/state_distro",
        hx_target=f"#{what}-report",
    )

    if total_jobs == 0:
        return Div(
            update_btn,
            Div(P(f"No {what} found."), cls="job-state-report"),
            cls="container",
        )

    state_percentages = {
        state: (count / total_jobs) * 100 for state, count in state_counts.items()
    }

    # Completed shown as a headline progress metric, with the remaining
    # operational categories as a bar chart (skipped if Bokeh is unavailable).
    # Completed is deliberately kept out of the chart so it does not dominate.
    chart = state_overview(dict(state_counts), what)

    # Create the report
    report = Div(
        chart,
        Table(
            Tr(Th("State"), Th("Count"), Th("Percentage")),
            *[
                Tr(
                    Td(state.name),
                    Td(str(count)),
                    Td(f"{state_percentages[state]:.2f}%"),
                )
                for state, count in state_counts.items()
            ],
            cls="state-distribution-table",
        ),
        cls="job-state-report",
    )
    return Div(
        update_btn,
        report,
        cls="container",
    )


@rt("/{proj_name}/{what}/info/{jf_id}")
def get_info_job_flow(jf_id: str, what: str, proj_name: str):
    info = None
    job_controller = get_job_controller(proj_name)

    if what == "jobs":
        job_info = job_controller.get_job_info(db_id=jf_id)
        info = job_info.model_dump() if job_info else None
    elif what == "flows":
        flow_info = job_controller.get_flows_info(flow_ids=[jf_id], limit=1)
        info = flow_info[0].model_dump() if flow_info else None

    label = "Flow" if what == "flows" else "Job"
    if info:
        return Div(
            H3(f"{label} {jf_id} details"),
            H3(f"{label} name: {info.pop('name')}"),
            ScrollableArea(
                *[
                    Ul(
                        Li(f"{k}:", cls="detail-key"),
                        Li(f"{v}", cls="detail-val"),
                    )
                    for k, v in info.items()
                ]
            ),
            cls="card-dialog active",
        )

    return P(f"{label} not found")


@rt("/{proj_name}/flows/graph/{jf_id}")
def get_graph_job_flow(jf_id: str, proj_name: str):
    job_controller = get_job_controller(proj_name)
    flowinfo = job_controller.get_flows_info(
        flow_ids=[jf_id], limit=1, with_jobs_info=True
    )[0]
    graph = get_mermaid(flowinfo)
    m_script = f"""
(async function() {{
    const container = document.getElementById('flow-graph');
    const {{ svg }} = await window.mermaid.render('graphDiv', `{graph}`);
    container.innerHTML = svg;
}})();
"""
    return Div(
        ScrollableArea(Div(id="flow-graph"), Script(m_script)), cls="card-dialog"
    )


@rt("/{proj_name}/{what}/dialog/{jf_id}")
def get_info_graph_dialog(jf_id: str, what: str, proj_name: str):
    return Dialog(
        Card(
            Button(
                "Close",
                hx_get="/test/close_dialog",
                hx_target="#dialog-container",
                style="font-weight: bold",
                cls="btn toolbar-right",
            ),
            Div(
                Button(
                    f"Details {what}",
                    cls="tab active",
                    hx_get=f"/{proj_name}/{what}/info/{jf_id}",
                    hx_target="#tab-content",
                    hx_swap="innerHTML",
                    _="on click remove .active from .tab then add .active to me",
                ),
                Button(
                    "Graph Flow",
                    cls="tab",
                    hx_get=f"/{proj_name}/flows/graph/{jf_id}",
                    hx_target="#tab-content",
                    hx_swap="innerHTML",
                    _="on click remove .active from .tab then add .active to me",
                ),
                cls="tab-buttons btn",
            )
            if what == "flows"
            else None,
            Div(
                get_info_job_flow(jf_id, what, proj_name),
                id="tab-content",
                cls="tab-content",
            ),
            cls="tabbed-card",
        ),
        id="my-dialog",
        open="open",
        cls="dialog",
    )


@rt("/{proj_name}/{what}/close_info")
def close_info():
    return ""


@rt("/{proj_name}/{what}/query")
def get(proj_name: str, what: str):
    form = Form(
        Group(
            Input(name="db_id", placeholder="DB ID")
            if what == "jobs"
            else Input(name="flow_id", placeholder="Flow ID"),
            Input(name="uuid", placeholder="UUID") if what == "jobs" else None,
            Input(name="name", placeholder="Job Name"),
            cls="group",
        ),
        Group(
            Label("State"),
            Select(
                Option("Any", value=""),
                *[
                    Option(s.name, value=s.value)
                    for s in (FlowState if what == "flows" else JobState)
                ],
                name="state",
                id="select-job-state",
            ),
            Input(name="worker", placeholder="Worker") if what == "jobs" else None,
            cls="group",
        ),
        # Date and time for start and end time
        Group(
            Label("Start Date/Time"),
            Input(type="date", name="start_date"),
            Input(type="time", name="start_time", label="Start Time"),
            Label("End Date/Time"),
            Input(type="date", name="end_date", label="End Date"),
            Input(type="time", name="end_time", label="End Time"),
            cls="group",
        ),
        # Input for entries per page
        Group(
            Label("Entries per page"),
            Select(
                *[
                    Option(str(i), value=str(i), selected=(i == 20))
                    for i in (10, 20, 50, 100)
                ],
                name="entries_per_page",
                id="select-npages",
            ),
            cls="group",
        ),
        # Hidden input to store the user's timezone (populated by JavaScript)
        Input(type="hidden", name="timezone_in", value="", id="timezone_in"),
        Input(type="hidden", name="sort_by", value="updated_on"),
        Input(type="hidden", name="sort_order", value="-1"),
        Button("Search", cls="btn"),
        Script(js_timezone),
        Script(mermaid_js, type="module"),
        hx_post=f"/{proj_name}/{what}/query",
        hx_target="#query-results",
        # Run the query once on initial load (in addition to manual submit) so
        # the table is populated with default values without an explicit search.
        hx_trigger="load, submit",
        cls="card",
        id="search-form",
    )

    job_controller = get_job_controller(proj_name)
    if what == "jobs":
        total_entries = job_controller.count_jobs()
    elif what == "flows":
        total_entries = job_controller.count_flows()

    return Div(
        H3(f"{what.capitalize()} Query"),
        P(f"Total number of {what}: {total_entries}", cls="muted"),
        form,
        Div(id="query-results"),
        id="prj-container",
    )


@rt("/{proj_name}/{what}/query")
def post(
    proj_name: str,
    what: str,
    db_id: str = "",
    flow_id: str = "",
    uuid: str = "",
    name: str = "",
    state: str = "",
    worker: str = "",
    start_date: str = "",
    start_time: str = "",
    end_date: str = "",
    end_time: str = "",
    timezone_in: str = "",
    sort_by: str = "updated_on",
    sort_order: int = -1,
    page: int = 1,
    entries_per_page: int = 20,
):
    query: dict[
        str,
        str
        | list[str]
        | tuple[str, None]
        | list[tuple[str, None]]
        | None
        | JobState
        | FlowState
        | dict[str, str]
        | datetime,
    ] = {}
    if db_id:
        query["db_ids"] = [db_id]
    if flow_id:
        query["flow_ids"] = [flow_id]
    # A uuid alone does not identify a single Job (the (uuid, index) pair does),
    # so match every index of that uuid via a generic query. Jobs-only: the
    # uuid field has no meaning for the flows query.
    custom_query = {"uuid": uuid} if uuid else None
    if name:
        query["name"] = name
    if state:
        query["states"] = FlowState(state) if what == "flows" else JobState(state)
    if worker:
        query["workers"] = [worker]

    tz = None
    if timezone_in:
        try:
            tz = ZoneInfo(timezone_in)
        except ZoneInfoNotFoundError:
            tz = None
    if tz is None:
        # Fall back to the local timezone of the machine running the GUI. This
        # is a fixed offset captured now (no IANA name needed), which is correct
        # for a local single-user GUI where the server clock is the user clock.
        tz = datetime.now(timezone.utc).astimezone().tzinfo  # type: ignore

    if start_time and not start_date:
        start_date = datetime.now().strftime("%Y-%m-%d")
    if start_date:
        if start_time:
            start_date = f"{start_date} {start_time}"
            query["start_date"] = datetime.strptime(start_date, "%Y-%m-%d %H:%M")
        else:
            query["start_date"] = datetime.strptime(start_date, "%Y-%m-%d")

    if end_time and not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if end_date:
        if end_time:
            end_date = f"{end_date} {end_time}"
            query["end_date"] = datetime.strptime(end_date, "%Y-%m-%d %H:%M")
        else:
            query["end_date"] = datetime.strptime(end_date, "%Y-%m-%d")

    skip = (page - 1) * entries_per_page

    job_controller = get_job_controller(proj_name)
    if what == "jobs":
        jobs_flows = job_controller.get_jobs_info(
            **query,
            custom_query=custom_query,
            sort=[[sort_by, sort_order]],
            limit=entries_per_page,
            skip=skip,
        )
    elif what == "flows":
        jobs_flows = job_controller.get_flows_info(
            **query, sort=[[sort_by, sort_order]], limit=entries_per_page, skip=skip
        )

    if not jobs_flows:
        return Div(P("No jobs found matching the criteria."), cls="card")

    # Toggle sort direction for next click
    next_sort_order = -sort_order

    # prepare all entries
    all_entries = list(jobs_flows)

    # prepare table
    info_table = Table(
        Tr(
            Th(
                A(
                    "DB id",
                    hx_post=f"/{proj_name}/{what}/query",
                    hx_include="previous form",
                    hx_vals=f'{{"sort_by":"db_id", "sort_order":"{next_sort_order if sort_by == "db_id" else 1}", "page":1}}',
                    hx_swap="innerHTML",
                    hx_target="#query-results",
                )
            )
            if what == "jobs"
            else None,
            Th(
                A(
                    "UUID",
                    hx_post=f"/{proj_name}/{what}/query",
                    hx_include="previous form",
                    hx_vals=f'{{"sort_by":"uuid", "sort_order":"{next_sort_order if sort_by == "uuid" else 1}", "page":1}}',
                    hx_swap="innerHTML",
                    hx_target="#query-results",
                )
            ),
            Th(
                A(
                    "Name",
                    hx_post=f"/{proj_name}/{what}/query",
                    hx_include="previous form",
                    hx_vals=f'{{"sort_by":"job.name", "sort_order":"{next_sort_order if sort_by == "job.name" else 1}", "page":1}}',
                    hx_swap="innerHTML",
                    hx_target="#query-results",
                )
            ),
            Th(
                A(
                    "State",
                    hx_post=f"/{proj_name}/{what}/query",
                    hx_include="previous form",
                    hx_vals=f'{{"sort_by":"state", "sort_order":"{next_sort_order if sort_by == "state" else 1}", "page":1}}',
                    hx_swap="innerHTML",
                    hx_target="#query-results",
                )
            ),
            Th(
                A(
                    "Worker",
                    hx_post=f"/{proj_name}/{what}/query",
                    hx_include="previous form",
                    hx_vals=f'{{"sort_by":"worker", "sort_order":"{next_sort_order if sort_by == "worker" else 1}", "page":1}}',
                    hx_swap="innerHTML",
                    hx_target="#query-results",
                )
            )
            if what == "jobs"
            else None,
            Th(
                A(
                    "Updated",
                    hx_post=f"/{proj_name}/{what}/query",
                    hx_include="previous form",
                    hx_vals=f'{{"sort_by":"updated_on", "sort_order":"{next_sort_order if sort_by == "updated_on" else 1}", "page":1}}',
                    hx_swap="innerHTML",
                    hx_target="#query-results",
                )
            ),
            Th(
                Label("Action", Input(type="checkbox", onclick="toggleAll(this)")),
                style="text-align: right",
            ),
        ),
        *[
            Tr(
                Td(
                    A(
                        entry.db_id,
                        hx_get=f"/{proj_name}/{what}/dialog/{entry.db_id}",
                        hx_target="#dialog-container",
                        hx_swap="innerHTML",
                    )
                )
                if what == "jobs"
                else None,
                Td(entry.uuid)
                if what == "jobs"
                else Td(
                    A(
                        entry.flow_id,
                        hx_get=f"/{proj_name}/{what}/dialog/{entry.flow_id}",
                        hx_target="#dialog-container",
                        hx_swap="innerHTML",
                    )
                ),
                Td(entry.name),
                Td(state_badge(entry.state)),
                Td(entry.worker) if what == "jobs" else None,
                Td(
                    entry.updated_on.replace(tzinfo=timezone.utc)
                    .astimezone(tz)
                    .strftime("%Y-%m-%d %H:%M:%S")
                ),
                Td(
                    Input(
                        type="checkbox",
                        name="ckbx_action",
                        value=f"{entry.db_id if what == 'jobs' else entry.flow_id}",
                        id=f"{entry.db_id if what == 'jobs' else entry.flow_id}",
                    ),
                    style="text-align: right",
                ),
            )
            for entry in all_entries
        ],
        cls="card",
    )

    if what == "jobs":
        total_entries = job_controller.count_jobs(**query, query=custom_query)
    elif what == "flows":
        total_entries = job_controller.count_flows(**query)

    total_pages = ceil(total_entries / entries_per_page)

    if what == "jobs":
        # all_job_flow_ids = [entry.db_id for entry in all_entries]
        action_btns_lbl = ("Rerun", "Resume", "Pause", "Stop", "Retry")
    elif what == "flows":
        # all_job_flow_ids = [entry.flow_id for entry in all_entries]
        action_btns_lbl = ("Delete",)  # type: ignore[assignment]

    # Destructive actions get the danger (red) button variant.
    danger_actions = {"Stop", "Delete"}
    action_btns = Group(
        Label("Actions:   "),
        *[
            Button(
                name,
                hx_post=f"/actions/{proj_name}/{name}/{what}/open_dialog",
                hx_target="#dialog-container",
                hx_include="[name='ckbx_action']",
                cls="btn btn-danger" if name in danger_actions else "btn",
            )
            for name in action_btns_lbl
        ],
        cls="group toolbar-right",
    )

    pagination = Div(
        A(
            "Previous",
            hx_post=f"/{proj_name}/{what}/query",
            hx_include="previous form",
            hx_vals=f'{{"page":{page - 1}, "sort_by":"{sort_by}", "sort_order":"{sort_order}"}}',
            hx_swap="innerHTML",
            hx_target="#query-results",
        )
        if page > 1
        else Span("Previous"),
        *[
            A(
                str(i),
                hx_post=f"/{proj_name}/{what}/query",
                hx_include="previous form",
                hx_vals=f'{{"page":{i}, "sort_by":"{sort_by}", "sort_order":"{sort_order}"}}',
                hx_swap="innerHTML",
                hx_target="#query-results",
            )
            for i in range(max(1, page - 2), min(total_pages + 1, page + 3))
        ],
        A(
            "Next",
            hx_post=f"/{proj_name}/{what}/query",
            hx_include="previous form",
            hx_vals=f'{{"page":{page + 1}, "sort_by":"{sort_by}", "sort_order":"{sort_order}"}}',
            hx_swap="innerHTML",
            hx_target="#query-results",
        )
        if page < total_pages
        else Span("Next"),
        cls="pagination",
    )

    toggle_all_ckbx = Script("""
        function toggleAll(source) {
        var checkboxes = document.querySelectorAll('input[type="checkbox"][name="ckbx_action"]');
        for (var i = 0; i < checkboxes.length; i++) {checkboxes[i].checked = source.checked;}}
    """)
    return Div(
        H4(f"Total after filter:{total_entries}"),
        action_btns,
        info_table,
        pagination,
        toggle_all_ckbx,
        id="query-results",
        cls="card",
    ), Div(Script(mermaid_js, type="module"), id="dialog-container")


@requires(
    fasthtml is not None, "The 'python-fasthtml' package is required to run the gui."
)
def start_gui(port: int | None = None):
    serve(
        appname="jobflow_remote.webgui.webgui",
        port=port,
        reload_includes=[Path(__file__).parent],
    )


if __name__ == "__main__":
    serve()
