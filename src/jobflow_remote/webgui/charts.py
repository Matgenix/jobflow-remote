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

# The state-color palette and the operational category definitions live in a
# separate, Bokeh-free module so they can be shared with the plain-HTML views in
# ``webgui``. Only the symbols used by the figure builders below are imported
# here; ``webgui`` imports the rest of the palette API directly from
# :mod:`jobflow_remote.webgui.palette`.
from jobflow_remote.webgui.palette import (
    _DEFAULT_COLOR,
    CATEGORY_COLORS,
    STATE_COLORS,
    categories_for,
)

if TYPE_CHECKING:
    from fastcore.xml import FT

    from jobflow_remote.jobs.report import FlowTrends, JobTrends


def _state_color(state_name: str) -> str:
    """Return the color for a state name, falling back to a neutral grey."""
    return STATE_COLORS.get(state_name, _DEFAULT_COLOR)


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
