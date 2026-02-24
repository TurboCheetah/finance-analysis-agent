from __future__ import annotations

from finance_analysis_agent.db.engine import get_engine


def test_get_engine_reuses_same_instance_for_same_url() -> None:
    url = "sqlite:///tmp/test-cache.db"

    engine_a = get_engine(url)
    engine_b = get_engine(url)

    assert engine_a is engine_b


def test_get_engine_returns_distinct_instances_for_different_urls() -> None:
    first = get_engine("sqlite:///tmp/test-cache-a.db")
    second = get_engine("sqlite:///tmp/test-cache-b.db")

    assert first is not second
