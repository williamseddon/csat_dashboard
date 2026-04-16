from __future__ import annotations

from review_analyst.social_listening import (
    _social_demo_payload,
    create_temp_search,
    fetch_mentions,
    run_full_analysis,
)


def test_run_full_analysis_returns_expected_modules() -> None:
    result = run_full_analysis(
        mw_key="",
        openai_key="",
        term="FlexStyle",
        days=30,
        sources=["Reddit", "YouTube", "Instagram", "TikTok", "Editorial"],
        fetch_count=50,
    )
    assert set(result) == {"viral", "paid", "love", "ideas", "hacks"}
    assert result["viral"]["spike_events"]
    assert result["paid"]["stats"]["total"] >= result["paid"]["stats"]["organic"]
    assert result["love"]["praise_themes"]
    assert result["ideas"]["improvement_themes"]
    assert result["hacks"]["hacks"]


def test_fetch_mentions_filters_by_source_and_sentiment() -> None:
    temp = create_temp_search("", "FlexStyle")
    assert temp is not None
    docs = fetch_mentions(
        key="",
        search_id=int(temp["id"]),
        days=30,
        sort_by="engagement",
        sources=["TikTok"],
        page_size=20,
        sentiments=["positive"],
    )
    assert docs
    assert {doc["source"]["source_type"] for doc in docs} == {"TikTok"}
    assert {doc["sentiment"] for doc in docs} == {"positive"}



def test_social_demo_payload_keeps_legacy_shapes() -> None:
    payload = _social_demo_payload("FlexStyle", days=30, sources=["Reddit", "YouTube", "Instagram", "TikTok", "Editorial"])
    assert {"posts", "top_comments", "detractors", "delighters", "viral", "compare", "metrics", "analysis"}.issubset(payload)
    assert not payload["posts"].empty
    assert not payload["top_comments"].empty
    assert not payload["viral"].empty
    assert payload["metrics"]["mentions"]
