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
