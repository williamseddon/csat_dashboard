from __future__ import annotations

import json
from typing import Any, Dict, List, Mapping

import pytest

from review_analyst.tag_quality import NegationDetector
from review_analyst import symptomizer as sym
from review_analyst.symptomizer import (
    SymptomSpec,
    _audit_tag_polarity,
    _format_rich_catalog,
    deduplicate_taxonomy_labels,
    prune_taxonomy,
    retry_zero_tag_reviews,
    tag_review_batch,
    tag_review_batch_v4,
    validate_evidence,
)


def _chat_returning(payload: Mapping[str, Any]):
    """Return a fake chat completion function that emits fixed JSON."""

    def _chat_complete_fn(*args: Any, **kwargs: Any) -> str:
        return json.dumps(payload)

    return _chat_complete_fn


@pytest.fixture(autouse=True)
def _clear_symptomizer_cache() -> None:
    """Reset the module cache so tests do not leak across runs."""

    sym._result_cache.clear()


def _run_single_pass_case(
    *,
    review_text: str,
    rating: Any,
    allowed_detractors: List[str],
    allowed_delighters: List[str],
    detractors: List[Dict[str, Any]],
    delighters: List[Dict[str, Any]],
) -> Dict[str, Any]:
    payload = {
        "items": [
            {
                "id": 0,
                "detractors": detractors,
                "delighters": delighters,
                "unlisted_detractors": [],
                "unlisted_delighters": [],
                "safety": "Not Mentioned",
                "reliability": "Not Mentioned",
                "sessions": "Unknown",
            }
        ]
    }
    out = tag_review_batch(
        client=None,
        items=[{"idx": 0, "review": review_text, "rating": rating}],
        allowed_detractors=allowed_detractors,
        allowed_delighters=allowed_delighters,
        chat_complete_fn=_chat_returning(payload),
        include_universal_neutral=False,
    )
    return out[0]


def test_format_rich_catalog_uses_guidance_blocks() -> None:
    catalog = _format_rich_catalog(
        [
            SymptomSpec(
                name="Hair Damage",
                desc="Reports of heat or breakage after use.",
                detractor_signal="reviewer reports breakage or dryness caused by the product",
                ambiguity_rule="skip general hair texture comments without product attribution",
                priority="high",
                aliases=["Damaged Hair", "Dry Hair"],
            )
        ],
        "detractor",
        ["Hair Damage"],
    )

    assert "[Hair Damage]" in catalog
    assert "desc: Reports of heat or breakage after use." in catalog
    assert "tag when: reviewer reports breakage or dryness caused by the product" in catalog
    assert "skip when: skip general hair texture comments without product attribution" in catalog
    assert "priority: HIGH" in catalog
    assert "aliases: Damaged Hair, Dry Hair" in catalog


def test_prune_taxonomy_moves_zero_hit_labels_to_extended() -> None:
    primary, extended = prune_taxonomy(
        ["Loud", "Hard To Clean", "Pricey", "Poor Durability"],
        {"Loud": 12, "Hard To Clean": 4, "Pricey": 0, "Poor Durability": 0},
        min_reviews_for_prune=2,
        max_labels=2,
    )

    assert primary == ["Loud", "Hard To Clean"]
    assert "Pricey" in extended
    assert "Poor Durability" in extended


def test_deduplicate_taxonomy_labels_adds_aliases_for_near_duplicates() -> None:
    deduped, aliases = deduplicate_taxonomy_labels(
        ["Loud Noise", "Noisy Motor", "Loud Motor", "Easy Setup"],
        aliases={"Easy Setup": ["Simple Setup"]},
        threshold=0.50,
    )

    assert "Loud Noise" in deduped
    assert "Easy Setup" in deduped
    assert "Noisy Motor" not in deduped
    assert "Loud Motor" not in deduped
    assert set(aliases["Loud Noise"]) >= {"Noisy Motor", "Loud Motor"}
    assert aliases["Easy Setup"] == ["Simple Setup"]


def test_validate_evidence_prefers_verbatim_review_span_for_fuzzy_match() -> None:
    review = "The cord is way too short for my bathroom counter and outlet placement."
    evidence = validate_evidence(["cord too short"], review, label="Short Cord")

    assert evidence
    assert evidence[0] in review
    assert "cord" in evidence[0].lower()
    assert "short" in evidence[0].lower()


def test_retry_zero_tag_reviews_respects_written_tag_keys() -> None:
    calls: List[int] = []

    def _chat_complete_fn(*args: Any, **kwargs: Any) -> str:
        calls.append(1)
        return json.dumps({"detractors": [], "delighters": []})

    results = {
        0: {
            "wrote_dets": ["Loud"],
            "wrote_dels": ["Quiet"],
            "ev_det": {"Loud": ["too loud"]},
            "ev_del": {"Quiet": ["quiet"]},
        }
    }

    retried = retry_zero_tag_reviews(
        client=None,
        results=results,
        items=[{"idx": 0, "review": "Quiet overall, but the old unit was loud.", "rating": 5}],
        allowed_detractors=["Loud"],
        allowed_delighters=["Quiet"],
        chat_complete_fn=_chat_complete_fn,
    )

    assert retried == results
    assert calls == []


def test_tag_review_batch_cache_invalidates_when_specs_change() -> None:
    calls: List[int] = []

    def _chat_complete_fn(*args: Any, **kwargs: Any) -> str:
        calls.append(1)
        return json.dumps(
            {
                "items": [
                    {
                        "id": 0,
                        "detractors": [],
                        "delighters": [{"label": "Quiet", "evidence": ["quiet"]}],
                        "unlisted_detractors": [],
                        "unlisted_delighters": [],
                        "safety": "Not Mentioned",
                        "reliability": "Not Mentioned",
                        "sessions": "Unknown",
                    }
                ]
            }
        )

    kwargs = dict(
        client=None,
        items=[{"idx": 0, "review": "This one is quiet.", "rating": 5}],
        allowed_detractors=["Loud"],
        allowed_delighters=["Quiet"],
        chat_complete_fn=_chat_complete_fn,
        include_universal_neutral=False,
    )

    first = tag_review_batch(**kwargs)
    second = tag_review_batch(**kwargs)
    third = tag_review_batch(
        **kwargs,
        delighter_specs=[
            SymptomSpec(
                name="Quiet",
                desc="Low-noise operation.",
                delighter_signal="reviewer says the product is quiet or low noise",
            )
        ],
    )

    assert first[0]["dels"] == ["Quiet"]
    assert second[0]["dels"] == ["Quiet"]
    assert third[0]["dels"] == ["Quiet"]
    assert len(calls) == 2


def test_negation_detector_covers_new_v4_patterns() -> None:
    assert NegationDetector.is_negated("quiet", "It is not as quiet as expected.")
    assert NegationDetector.is_negated("quiet", "This would be better if more quiet.")
    assert NegationDetector.is_negated("quiet", "It only quiet on low speed.")
    assert NegationDetector.is_negated("durable", "This was supposed to be durable but broke fast.")


@pytest.mark.parametrize(
    ("label", "evidence", "side", "review_text", "expected"),
    [
        (
            "Hair Damage",
            ["damage my hair"],
            "detractor",
            "I was worried it would damage my hair but it didn't.",
            "inverted",
        ),
        (
            "Loud",
            ["OLD dryer was so loud"],
            "detractor",
            "My OLD dryer was so loud, this one is quiet.",
            "inverted",
        ),
        (
            "Quiet",
            ["quiet"],
            "delighter",
            "Not as quiet as expected.",
            "inverted",
        ),
        (
            "Hard To Clean",
            ["cleanup takes forever"],
            "detractor",
            "Works great but cleanup takes forever.",
            "correct",
        ),
    ],
)
def test_audit_tag_polarity_flags_inversion_and_valid_cases(
    label: str,
    evidence: List[str],
    side: str,
    review_text: str,
    expected: str,
) -> None:
    assert _audit_tag_polarity(label, evidence, side, review_text) == expected


@pytest.mark.parametrize(
    (
        "review_text",
        "rating",
        "allowed_detractors",
        "allowed_delighters",
        "raw_detractors",
        "raw_delighters",
        "expected_detractors",
        "expected_delighters",
    ),
    [
        (
            "No issues with noise.",
            5,
            ["Loud"],
            ["Quiet"],
            [{"label": "Loud", "evidence": ["noise"]}],
            [{"label": "Quiet", "evidence": ["No issues with noise"]}],
            [],
            ["Quiet"],
        ),
        (
            "Not as quiet as expected.",
            3,
            ["Loud"],
            ["Quiet"],
            [{"label": "Loud", "evidence": ["not as quiet as expected"]}],
            [{"label": "Quiet", "evidence": ["quiet"]}],
            ["Loud"],
            [],
        ),
        (
            "Great value, but not durable.",
            4,
            ["Poor Durability"],
            ["Good Value", "Reliable"],
            [{"label": "Poor Durability", "evidence": ["not durable"]}],
            [
                {"label": "Good Value", "evidence": ["Great value"]},
                {"label": "Reliable", "evidence": ["durable"]},
            ],
            ["Poor Durability"],
            ["Good Value"],
        ),
        (
            "I was worried it would damage my hair but it didn't.",
            5,
            ["Hair Damage"],
            ["No Hair Damage"],
            [{"label": "Hair Damage", "evidence": ["damage my hair"]}],
            [{"label": "No Hair Damage", "evidence": ["it didn't damage my hair"]}],
            [],
            ["No Hair Damage"],
        ),
        (
            "Works great but cleanup takes forever.",
            4,
            ["Hard To Clean"],
            ["Overall Satisfaction"],
            [{"label": "Hard To Clean", "evidence": ["cleanup takes forever"]}],
            [{"label": "Overall Satisfaction", "evidence": ["Works great"]}],
            ["Hard To Clean"],
            ["Overall Satisfaction"],
        ),
        (
            "Easy to use once you figure out the setup.",
            4,
            ["Difficult Setup"],
            ["Easy To Use"],
            [{"label": "Difficult Setup", "evidence": ["figure out the setup"]}],
            [{"label": "Easy To Use", "evidence": ["Easy to use"]}],
            ["Difficult Setup"],
            ["Easy To Use"],
        ),
        (
            "My OLD dryer was so loud, this one is quiet.",
            5,
            ["Loud"],
            ["Quiet"],
            [{"label": "Loud", "evidence": ["OLD dryer was so loud"]}],
            [{"label": "Quiet", "evidence": ["this one is quiet"]}],
            [],
            ["Quiet"],
        ),
        (
            "Great, another broken product.",
            1,
            ["Broke Quickly"],
            [],
            [{"label": "Broke Quickly", "evidence": ["broken product"]}],
            [],
            ["Broke Quickly"],
            [],
        ),
        (
            "Loved at first, now broken after 2 weeks.",
            1,
            ["Broke Quickly"],
            [],
            [{"label": "Broke Quickly", "evidence": ["broken after 2 weeks"]}],
            [],
            ["Broke Quickly"],
            [],
        ),
        (
            "Kind of loud but manageable.",
            3,
            ["Loud"],
            [],
            [{"label": "Loud", "evidence": ["Kind of loud"]}],
            [],
            ["Loud"],
            [],
        ),
    ],
)
def test_single_pass_symptomizer_handles_v4_regression_reviews(
    review_text: str,
    rating: Any,
    allowed_detractors: List[str],
    allowed_delighters: List[str],
    raw_detractors: List[Dict[str, Any]],
    raw_delighters: List[Dict[str, Any]],
    expected_detractors: List[str],
    expected_delighters: List[str],
) -> None:
    result = _run_single_pass_case(
        review_text=review_text,
        rating=rating,
        allowed_detractors=allowed_detractors,
        allowed_delighters=allowed_delighters,
        detractors=raw_detractors,
        delighters=raw_delighters,
    )

    assert result["dets"] == expected_detractors
    assert result["dels"] == expected_delighters


def test_tag_review_batch_v4_routes_short_and_long_reviews(monkeypatch: pytest.MonkeyPatch) -> None:
    call_batches: List[List[int]] = []

    def fake_single_pass(*, items: List[Mapping[str, Any]], **kwargs: Any) -> Dict[int, Dict[str, Any]]:
        call_batches.append([int(item["idx"]) for item in items])
        out: Dict[int, Dict[str, Any]] = {}
        for item in items:
            idx = int(item["idx"])
            out[idx] = {
                "dets": ["Single Pass Label"] if idx == 2 else ["Short Review Label"],
                "dels": [],
                "ev_det": {"Single Pass Label": ["single-pass evidence"]} if idx == 2 else {"Short Review Label": ["short evidence"]},
                "ev_del": {},
                "unl_dets": [],
                "unl_dels": [],
                "safety": "Not Mentioned",
                "reliability": "Not Mentioned",
                "sessions": "Unknown",
            }
        return out

    def fake_extract_claims(**kwargs: Any) -> List[Dict[str, str]]:
        return [{"text": "cleanup takes forever", "polarity": "negative", "aspect": "Hard To Clean"}]

    def fake_map_claims_to_taxonomy(
        claims: List[Dict[str, str]],
        allowed_detractors: List[str],
        allowed_delighters: List[str],
        aliases: Dict[str, List[str]] | None = None,
    ) -> tuple[List[str], List[str], Dict[str, List[str]], Dict[str, List[str]]]:
        return ["Hard To Clean"], [], {"Hard To Clean": ["cleanup takes forever"]}, {}

    monkeypatch.setattr(sym, "tag_review_batch", fake_single_pass)
    monkeypatch.setattr(sym, "extract_claims", fake_extract_claims)
    monkeypatch.setattr(sym, "map_claims_to_taxonomy", fake_map_claims_to_taxonomy)
    monkeypatch.setattr(sym, "needs_verification", lambda result, review_text, rating: False)

    items = [
        {"idx": 1, "review": "Too loud.", "rating": 2},
        {"idx": 2, "review": "This machine works great overall, but the cleanup takes forever because grease gets stuck in every corner." * 8, "rating": 3},
    ]

    out = tag_review_batch_v4(
        client=None,
        items=items,
        allowed_detractors=["Hard To Clean", "Short Review Label", "Single Pass Label"],
        allowed_delighters=["Quiet"],
        long_review_threshold=100,
    )

    assert call_batches == [[1], [2]]
    assert out[1]["dets"] == ["Short Review Label"]
    assert out[2]["dets"] == ["Hard To Clean", "Single Pass Label"]
    assert out[2]["ev_det"]["Hard To Clean"] == ["cleanup takes forever"]
    assert out[2]["ev_det"]["Single Pass Label"] == ["single-pass evidence"]
