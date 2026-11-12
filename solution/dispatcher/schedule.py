"""Pure scheduling rules.  Keeping these separate makes the risky time maths testable."""
from __future__ import annotations

from datetime import datetime, time, timedelta

DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


def parse_hm(value: str) -> time:
    hour, minute = value.split(":")
    return time(int(hour), int(minute))


def in_window(now: datetime, campaign: dict, regulatory: dict) -> bool:
    """Whether a new call may start now (end points are deliberately exclusive)."""
    local = now
    window = campaign["window"]
    return (
        DAYS[local.weekday()] in window["days"]
        and parse_hm(window["start"]) <= local.timetz().replace(tzinfo=None) < parse_hm(window["end"])
        and parse_hm(regulatory["start"]) <= local.timetz().replace(tzinfo=None) < parse_hm(regulatory["end"])
    )


def next_campaign_open(at: datetime, campaign: dict, regulatory: dict) -> datetime:
    """First allowed start at or after *at*, accounting for campaign and regulatory windows."""
    window = campaign["window"]
    start = max(parse_hm(window["start"]), parse_hm(regulatory["start"]))
    end = min(parse_hm(window["end"]), parse_hm(regulatory["end"]))
    probe = at
    for offset in range(9):
        day = (at + timedelta(days=offset)).date()
        candidate = datetime.combine(day, start, tzinfo=at.tzinfo)
        close = datetime.combine(day, end, tzinfo=at.tzinfo)
        if DAYS[candidate.weekday()] not in window["days"]:
            continue
        if offset == 0 and probe >= close:
            continue
        if offset == 0 and candidate <= probe < close:
            return probe
        if candidate >= probe:
            return candidate
    raise ValueError("campaign has no opening in the next nine days")


def retry_due(ended_at: datetime, status: str, policy: dict) -> datetime:
    minutes = {"no_answer": policy["no_answer_minutes"], "busy": policy["busy_minutes"],
               "failed": policy["failed_minutes"]}[status]
    return ended_at + timedelta(minutes=minutes)


def priority(kind: str) -> int:
    """Lower is more urgent: due callbacks, then retries, then never-called leads."""
    return {"callback": 0, "retry": 1, "fresh": 2}[kind]
