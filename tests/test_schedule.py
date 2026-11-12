from datetime import datetime, timedelta, timezone

from solution.dispatcher.schedule import in_window, next_campaign_open, priority, retry_due

IST = timezone(timedelta(hours=5, minutes=30))
REGULATORY = {"start": "09:00", "end": "21:00"}
CAMPAIGN = {"window": {"start": "10:00", "end": "19:00", "days": ["mon", "tue", "wed", "thu", "fri", "sat"]}}


def at(day: int, hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 11, day, hour, minute, tzinfo=IST)


def test_window_has_exclusive_end_and_allowed_days():
    assert in_window(at(12, 10), CAMPAIGN, REGULATORY)  # Thursday
    assert not in_window(at(12, 9, 59), CAMPAIGN, REGULATORY)
    assert not in_window(at(12, 19), CAMPAIGN, REGULATORY)
    assert not in_window(at(15, 12), CAMPAIGN, REGULATORY)  # Sunday


def test_next_open_moves_to_campaign_open_and_skips_sunday():
    assert next_campaign_open(at(12, 8), CAMPAIGN, REGULATORY) == at(12, 10)
    assert next_campaign_open(at(12, 18), CAMPAIGN, REGULATORY) == at(12, 18)
    assert next_campaign_open(at(14, 20), CAMPAIGN, REGULATORY) == at(16, 10)


def test_retry_intervals_and_priority_order():
    policy = {"no_answer_minutes": 60, "busy_minutes": 60, "failed_minutes": 15}
    assert retry_due(at(12, 12), "failed", policy) == at(12, 12, 15)
    assert retry_due(at(12, 12), "busy", policy) == at(12, 13)
    assert priority("callback") < priority("retry") < priority("fresh")
