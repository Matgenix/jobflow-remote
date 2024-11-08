import pytest


def test_get_past_time_rounded():
    from datetime import datetime

    from jobflow_remote.utils.data import get_past_time_rounded

    ref_date = datetime.fromisoformat("2024-09-16T16:53:36.414174")

    past = get_past_time_rounded(interval="hours", num_intervals=2, reference=ref_date)
    assert ref_date.replace(hour=15, minute=0, second=0, microsecond=0) == past

    past = get_past_time_rounded(interval="days", num_intervals=1, reference=ref_date)
    assert ref_date.replace(day=16, hour=0, minute=0, second=0, microsecond=0) == past

    past = get_past_time_rounded(interval="days", num_intervals=5, reference=ref_date)
    assert ref_date.replace(day=12, hour=0, minute=0, second=0, microsecond=0) == past

    past = get_past_time_rounded(interval="weeks", num_intervals=1, reference=ref_date)
    assert ref_date.replace(day=16, hour=0, minute=0, second=0, microsecond=0) == past

    past = get_past_time_rounded(interval="weeks", num_intervals=3, reference=ref_date)
    assert ref_date.replace(day=2, hour=0, minute=0, second=0, microsecond=0) == past

    past = get_past_time_rounded(interval="months", num_intervals=1, reference=ref_date)
    assert (
        ref_date.replace(month=9, day=1, hour=0, minute=0, second=0, microsecond=0)
        == past
    )

    past = get_past_time_rounded(interval="months", num_intervals=3, reference=ref_date)
    assert (
        ref_date.replace(month=7, day=1, hour=0, minute=0, second=0, microsecond=0)
        == past
    )

    past = get_past_time_rounded(interval="years", num_intervals=1, reference=ref_date)
    assert (
        ref_date.replace(
            year=2024, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        == past
    )

    past = get_past_time_rounded(interval="years", num_intervals=4, reference=ref_date)
    assert (
        ref_date.replace(
            year=2021, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        == past
    )


def test_get_utc_offset():
    from jobflow_remote.utils.data import get_utc_offset

    assert get_utc_offset("America/Los_Angeles") == "-07:00"
    assert get_utc_offset("Europe/Moscow") == "+03:00"
    assert get_utc_offset("UTC") == "+00:00"
    assert get_utc_offset("Asia/Shanghai") == "+08:00"
    with pytest.raises(ValueError, match="Could not determine the timezone for XXX"):
        get_utc_offset("XXX")
