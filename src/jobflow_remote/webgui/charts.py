"""Bokeh chart builders for the web GUI.

This module keeps the figure-building logic separate from the FastHTML routes
in :mod:`jobflow_remote.webgui.webgui`. Each public function turns a piece of
the cached report data (see :mod:`jobflow_remote.jobs.report`) into a standalone
Bokeh :class:`~bokeh.plotting.figure`. The :func:`bokeh_chart` helper wraps a
figure in the small amount of HTML/JS needed to render it through Bokeh's
standalone embedding API, in a way that also works when the fragment is swapped
into the page by HTMX.
"""

from __future__ import annotations

import json
import uuid
from math import pi
from typing import TYPE_CHECKING

from bokeh.embed import json_item
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
from fasthtml.common import Div, Script

if TYPE_CHECKING:
    from fastcore.xml import FT

    from jobflow_remote.jobs.report import FlowTrends, JobTrends
    from jobflow_remote.jobs.state import FlowState, JobState

# Color associated with each state, keyed by the state *name* so that the same
# map can be shared by both ``JobState`` and ``FlowState`` (their members
# overlap by name). Greens are terminal-success, reds/oranges are error states,
# warm tones are "active", and greys are idle/inactive.
STATE_COLORS: dict[str, str] = {
    "WAITING": "#95a5a6",
    "READY": "#5dade2",
    "CHECKED_OUT": "#48c9b0",
    "UPLOADED": "#45b39d",
    "SUBMITTED": "#5499c7",
    "RUNNING": "#f39c12",
    "RUN_FINISHED": "#16a085",
    "DOWNLOADED": "#1abc9c",
    "REMOTE_ERROR": "#e67e22",
    "COMPLETED": "#2ecc71",
    "FAILED": "#e74c3c",
    "PAUSED": "#9b59b6",
    "STOPPED": "#7f8c8d",
    "USER_STOPPED": "#34495e",
    "BATCH_SUBMITTED": "#85c1e9",
    "BATCH_RUNNING": "#f8c471",
}

_DEFAULT_COLOR = "#bdc3c7"

# Operational categories grouping the raw states. ``COMPLETED`` is deliberately
# excluded: it is reported separately as a headline metric so that the
# (continuously growing) pile of completed jobs does not dominate the chart.
# The list order is the display order (top to bottom) of the bar chart.
#
# Note the distinction between "Pending" (``WAITING``/``READY``: not yet started
# by jobflow-remote) and "Queued" (``SUBMITTED``/``BATCH_SUBMITTED``: actually
# sitting in the HPC scheduler queue, e.g. SLURM/PBS). Jobs and flows use
# different category sets because ``FlowState`` has no scheduler/transfer states.
JOB_CATEGORIES: list[tuple[str, list[str]]] = [
    ("Pending", ["WAITING", "READY"]),
    ("Queued", ["SUBMITTED", "BATCH_SUBMITTED"]),
    (
        "Active",
        [
            "CHECKED_OUT",
            "UPLOADED",
            "RUNNING",
            "RUN_FINISHED",
            "DOWNLOADED",
            "BATCH_RUNNING",
        ],
    ),
    ("Error", ["FAILED", "REMOTE_ERROR"]),
    ("Paused/Stopped", ["PAUSED", "STOPPED", "USER_STOPPED"]),
]

FLOW_CATEGORIES: list[tuple[str, list[str]]] = [
    ("Pending", ["WAITING", "READY"]),
    ("Running", ["RUNNING"]),
    ("Error", ["FAILED"]),
    ("Paused/Stopped", ["PAUSED", "STOPPED"]),
]

# Representative color for each category (reusing the per-state palette).
CATEGORY_COLORS: dict[str, str] = {
    "Pending": STATE_COLORS["WAITING"],
    "Queued": STATE_COLORS["SUBMITTED"],
    "Active": STATE_COLORS["RUNNING"],
    "Running": STATE_COLORS["RUNNING"],
    "Error": STATE_COLORS["FAILED"],
    "Paused/Stopped": STATE_COLORS["PAUSED"],
}


def _state_color(state_name: str) -> str:
    """Return the color for a state name, falling back to a neutral grey."""
    return STATE_COLORS.get(state_name, _DEFAULT_COLOR)


def categories_for(what: str) -> list[tuple[str, list[str]]]:
    """Return the ordered category definition for ``"jobs"`` or ``"flows"``."""
    return FLOW_CATEGORIES if what == "flows" else JOB_CATEGORIES


def categorize_state_counts(
    state_counts: dict[JobState | FlowState, int], what: str = "jobs"
) -> tuple[int, int, dict[str, int]]:
    """
    Split a state-count mapping into the headline numbers and category counts.

    Parameters
    ----------
    state_counts
        Mapping of state to the number of jobs/flows in that state.
    what
        Either ``"jobs"`` or ``"flows"``, selecting the category definition.

    Returns
    -------
    tuple of (int, int, dict)
        The number of completed jobs/flows, the total number, and a mapping of
        operational category name to count (excluding completed). All categories
        for the chosen entity are present, even when their count is zero.
    """
    by_name = {state.name: count for state, count in state_counts.items()}
    total = sum(by_name.values())
    completed = by_name.get("COMPLETED", 0)
    categories = {
        category: sum(by_name.get(name, 0) for name in names)
        for category, names in categories_for(what)
    }
    return completed, total, categories


def bokeh_chart(fig: figure) -> FT:
    """
    Wrap a Bokeh figure in the markup needed to render it in the page.

    The figure is serialized with :func:`bokeh.embed.json_item` and rendered
    client-side via ``Bokeh.embed.embed_item``. A fresh, unique target ``id`` is
    generated on every call so repeated HTMX swaps never collide, and the render
    call retries until ``window.Bokeh`` is available (BokehJS is loaded once in
    the page headers, but the script may run before it has finished loading).

    Parameters
    ----------
    fig
        The Bokeh figure to embed.

    Returns
    -------
    FT
        A FastHTML ``Div`` containing the target element and its render script.
    """
    div_id = f"bk-{uuid.uuid4().hex}"
    item_json = json.dumps(json_item(fig, div_id))
    script = Script(
        f"""
(function() {{
    const item = {item_json};
    function render() {{
        if (window.Bokeh && window.Bokeh.embed) {{
            window.Bokeh.embed.embed_item(item);
        }} else {{
            setTimeout(render, 50);
        }}
    }}
    render();
}})();
"""
    )
    return Div(Div(id=div_id), script, cls="bokeh-chart")


def category_bar_figure(
    categories: dict[str, int],
    what: str = "jobs",
    title: str = "In progress & attention",
) -> figure:
    """
    Build a horizontal bar chart of the non-completed state categories.

    Completed jobs/flows are intentionally not represented here (they are shown
    as a separate headline metric); this keeps the small, operationally relevant
    counts visible instead of being dwarfed by the completed total.

    Parameters
    ----------
    categories
        Mapping of category name to count, as produced by
        :func:`categorize_state_counts`. All categories are drawn even when zero.
    what
        Either ``"jobs"`` or ``"flows"``, selecting the category order.
    title
        Title displayed above the chart.

    Returns
    -------
    figure
        A Bokeh horizontal-bar figure.
    """
    # ``y_range`` lists categories bottom-to-top, so reverse the display order to
    # keep the first category at the top.
    names = [name for name, _ in categories_for(what)]
    y_range = list(reversed(names))
    counts = [categories.get(name, 0) for name in y_range]
    colors = [CATEGORY_COLORS.get(name, _DEFAULT_COLOR) for name in y_range]

    source = ColumnDataSource({"category": y_range, "count": counts, "color": colors})

    p = figure(
        height=220,
        sizing_mode="stretch_width",
        title=title,
        y_range=y_range,
        toolbar_location=None,
        tools="hover",
        tooltips="@category: @count",
    )
    p.hbar(
        y="category",
        right="count",
        height=0.7,
        fill_color="color",
        line_color=None,
        source=source,
    )
    p.x_range.start = 0
    p.ygrid.grid_line_color = None
    p.outline_line_color = None
    return p


def trends_figure(
    trends: JobTrends | FlowTrends, title: str = "Throughput over time"
) -> figure:
    """
    Build a stacked bar chart of completed/failed (and remote-error) trends.

    Parameters
    ----------
    trends
        The trends data from a jobs or flows report. ``JobTrends`` additionally
        carries a ``remote_error`` series, which is included when present.
    title
        Title displayed above the chart.

    Returns
    -------
    figure
        A Bokeh stacked vertical-bar figure over the trend dates.
    """
    data: dict[str, list] = {
        "dates": list(trends.dates),
        "Completed": list(trends.completed),
        "Failed": list(trends.failed),
    }
    stackers = ["Completed", "Failed"]
    colors = [_state_color("COMPLETED"), _state_color("FAILED")]

    remote_error = getattr(trends, "remote_error", None)
    if remote_error is not None:
        data["Remote error"] = list(remote_error)
        stackers.append("Remote error")
        colors.append(_state_color("REMOTE_ERROR"))

    p = figure(
        height=320,
        sizing_mode="stretch_width",
        title=title,
        x_range=list(trends.dates),
        toolbar_location=None,
        tools="hover",
        tooltips="$name @dates: @$name",
    )
    p.vbar_stack(
        stackers,
        x="dates",
        width=0.8,
        color=colors,
        source=data,
        legend_label=stackers,
    )
    p.y_range.start = 0
    p.xgrid.grid_line_color = None
    p.xaxis.major_label_orientation = pi / 4
    p.outline_line_color = None
    p.legend.location = "top_left"
    p.legend.orientation = "horizontal"
    return p


def worker_utilization_figure(worker_utilization: dict[str | None, int]) -> figure:
    """
    Build a horizontal bar chart of the number of jobs assigned per worker.

    Parameters
    ----------
    worker_utilization
        Mapping of worker name to the number of jobs assigned to it. A ``None``
        key (jobs with no worker) is rendered as ``"unassigned"``.

    Returns
    -------
    figure
        A Bokeh horizontal-bar figure.
    """
    # Sort ascending so the busiest worker ends up at the top of the chart.
    ordered = sorted(worker_utilization.items(), key=lambda kv: kv[1])
    workers = [name if name is not None else "unassigned" for name, _ in ordered]
    counts = [count for _, count in ordered]

    source = ColumnDataSource({"workers": workers, "counts": counts})

    p = figure(
        height=max(160, 40 * len(workers)),
        sizing_mode="stretch_width",
        title="Worker utilization",
        y_range=workers,
        toolbar_location=None,
        tools="hover",
        tooltips="@workers: @counts",
    )
    p.hbar(
        y="workers",
        right="counts",
        height=0.7,
        fill_color=_state_color("RUNNING"),
        line_color=None,
        source=source,
    )
    p.x_range.start = 0
    p.ygrid.grid_line_color = None
    p.outline_line_color = None
    return p
