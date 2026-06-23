"""Shared color palette for the web GUI.

This module is intentionally dependency-free (no Bokeh, no FastHTML) so that the
state colors can be reused both by the optional Bokeh charts
(:mod:`jobflow_remote.webgui.charts`) and by the plain-HTML state badges in
:mod:`jobflow_remote.webgui.webgui`, even when the optional ``gui`` extra does
not provide Bokeh.

The colors are keyed by the state *name* so that the same map can be shared by
both ``JobState`` and ``FlowState`` (their members overlap by name). Greens are
terminal-success, reds/oranges are error states, warm tones are "active", and
greys are idle/inactive.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from jobflow_remote.jobs.state import FlowState, JobState

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


def state_color(state_name: str) -> str:
    """
    Return the color for a state name, falling back to a neutral grey.

    Parameters
    ----------
    state_name
        The name of the ``JobState`` or ``FlowState`` member.

    Returns
    -------
    str
        A hex color string for the state, or a neutral grey when unknown.
    """
    return STATE_COLORS.get(state_name, _DEFAULT_COLOR)


# Operational categories grouping the raw states. ``COMPLETED`` is deliberately
# excluded: it is reported separately as a headline metric so that the
# (continuously growing) pile of completed jobs does not dominate the chart.
# The list order is the display order.
#
# Note the distinction between "Pending" (``WAITING``/``READY``: not yet started
# by jobflow-remote) and "Queued" (``SUBMITTED``/``BATCH_SUBMITTED``: actually
# sitting in the HPC scheduler queue, e.g. SLURM/PBS). Jobs and flows use
# different category sets because ``FlowState`` has no scheduler/transfer states.
#
# These live in this dependency-free module (rather than in the Bokeh-backed
# ``charts`` module) so the plain-HTML views can reuse them even without Bokeh.
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
