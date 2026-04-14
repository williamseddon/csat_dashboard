import pytest

from review_analyst.tag_quality import (
    UNIVERSAL_NEUTRAL_DETRACTORS,
    UNIVERSAL_NEUTRAL_DELIGHTERS,
    compute_tag_edit_accuracy,
    ensure_universal_taxonomy,
    normalize_tag_list,
    is_universal_neutral_label,
    refine_tag_assignment,
    strip_universal_neutral_tags,
)


def test_refine_tag_assignment_drops_unsupported_opposite_tag():
    refined = refine_tag_assignment(
        "Cooks great, a little noisy. I love the functions and how easy-to-use and clean it is.",
        detractors=["Loud"],
        delighters=["Quiet"],
        allowed_detractors=["Loud"],
        allowed_delighters=["Quiet"],
        evidence_det={"Loud": ["a little noisy"]},
        evidence_del={"Quiet": ["great cookware"]},
    )

    assert refined["dets"] == ["Loud"]
    assert "Overall Satisfaction" not in refined["dels"]
    assert refined["ev_det"]["Loud"] == ["a little noisy"]


def test_refine_tag_assignment_can_add_clearly_supported_missing_tag():
    refined = refine_tag_assignment(
        "Works great and it is a lot quieter than my last one.",
        detractors=[],
        delighters=[],
        allowed_detractors=["Loud"],
        allowed_delighters=["Quiet"],
        evidence_det={},
        evidence_del={},
    )

    assert "Quiet" in refined["dels"]
    assert "Overall Satisfaction" not in refined["dels"]
    assert refined["dets"] == []
    assert refined["ev_del"]["Quiet"]


def test_compute_tag_edit_accuracy_matches_add_and_remove_formula():
    baseline = {str(i): {"detractors": [f"Tag {i}"], "delighters": []} for i in range(1, 31)}
    current = {key: {"detractors": list(value["detractors"]), "delighters": list(value["delighters"])} for key, value in baseline.items()}

    current["1"]["detractors"] = []
    current["2"]["detractors"] = []
    current["29"]["delighters"] = ["New Strength A"]
    current["30"]["delighters"] = ["New Strength B"]

    metrics = compute_tag_edit_accuracy(baseline, current)

    assert metrics["baseline_total_tags"] == 30
    assert metrics["added_tags"] == 2
    assert metrics["removed_tags"] == 2
    assert metrics["accuracy_pct"] == pytest.approx(86.7, abs=0.05)


def test_refine_tag_assignment_adds_overall_satisfaction_for_generic_positive_review():
    refined = refine_tag_assignment(
        "This is the second Ninja air fryer I've bought, and it's great.",
        detractors=[],
        delighters=[],
        allowed_detractors=["Overall Dissatisfaction"],
        allowed_delighters=["Overall Satisfaction"],
        evidence_det={},
        evidence_del={},
    )

    assert refined["dels"] == ["Overall Satisfaction"]
    assert refined["dets"] == []
    assert refined["ev_del"]["Overall Satisfaction"]


def test_refine_tag_assignment_matches_curly_apostrophes_for_sentiment_fallback():
    refined = refine_tag_assignment(
        "This is the second Ninja air fryer I’ve bought, and it’s great.",
        detractors=[],
        delighters=[],
        allowed_detractors=["Overall Dissatisfaction"],
        allowed_delighters=["Overall Satisfaction"],
        evidence_det={},
        evidence_del={},
    )

    assert refined["dels"] == ["Overall Satisfaction"]
    assert refined["ev_del"]["Overall Satisfaction"]


def test_ensure_universal_taxonomy_injects_universal_neutral_pack():
    dets, dels = ensure_universal_taxonomy(["Loud"], ["Quiet"])

    assert dets[0] == "Overall Dissatisfaction"
    assert "Loud" in dets
    assert "Overpriced" in dets
    assert dels[0] == "Overall Satisfaction"
    assert "Quiet" in dels
    assert "Good Value" in dels


def test_ensure_universal_taxonomy_can_be_disabled():
    dets, dels = ensure_universal_taxonomy(["Loud"], ["Quiet"], include_universal_neutral=False)

    assert dets == ["Loud"]
    assert dels == ["Quiet"]


def test_strip_universal_neutral_tags_removes_pack_only():
    dets, dels = strip_universal_neutral_tags(
        UNIVERSAL_NEUTRAL_DETRACTORS + ["Loud"],
        UNIVERSAL_NEUTRAL_DELIGHTERS + ["Quiet"],
    )

    assert dets == ["Loud"]
    assert dels == ["Quiet"]


def test_custom_universal_neutral_tags_are_injected_and_detected():
    dets, dels = ensure_universal_taxonomy(
        ["Loud"],
        ["Quiet"],
        extra_universal_detractors=["Poor Packaging"],
        extra_universal_delighters=["Well Packaged"],
    )

    assert "Poor Packaging" in dets
    assert "Well Packaged" in dels
    assert is_universal_neutral_label("Poor Packaging", extra_universal_detractors=["Poor Packaging"])
    assert is_universal_neutral_label("Well Packaged", extra_universal_delighters=["Well Packaged"])


def test_refine_tag_assignment_can_add_overall_satisfaction_even_if_catalog_omits_it():
    refined = refine_tag_assignment(
        "This is the second Ninja air fryer I've bought, and it's great.",
        detractors=[],
        delighters=[],
        allowed_detractors=["Loud"],
        allowed_delighters=["Quiet"],
        evidence_det={},
        evidence_del={},
    )

    assert "Overall Satisfaction" in refined["dels"]


def test_refine_tag_assignment_can_add_overall_dissatisfaction_for_generic_negative_review():
    refined = refine_tag_assignment(
        "Very disappointed. It does not work and I would not recommend it.",
        detractors=[],
        delighters=[],
        allowed_detractors=["Loud"],
        allowed_delighters=["Quiet"],
        evidence_det={},
        evidence_del={},
    )

    assert "Overall Dissatisfaction" in refined["dets"]
    assert refined["dels"] == []


def test_refine_tag_assignment_uses_rating_for_broad_sentiment_fallback():
    refined = refine_tag_assignment(
        "Second unit and still happy.",
        detractors=[],
        delighters=[],
        allowed_detractors=[],
        allowed_delighters=[],
        evidence_det={},
        evidence_del={},
        rating=5,
    )

    assert "Overall Satisfaction" in refined["dels"]


def test_refine_tag_assignment_can_add_value_and_performance_universals():
    refined = refine_tag_assignment(
        "Great value for the price and it performs well every time.",
        detractors=[],
        delighters=[],
        allowed_detractors=[],
        allowed_delighters=[],
        evidence_det={},
        evidence_del={},
    )

    assert "Good Value" in refined["dels"]
    assert "Performs Well" in refined["dels"]


def test_refine_tag_assignment_respects_disabled_universal_neutral_pack():
    refined = refine_tag_assignment(
        "Great value for the price and it performs well every time.",
        detractors=[],
        delighters=[],
        allowed_detractors=[],
        allowed_delighters=[],
        evidence_det={},
        evidence_del={},
        include_universal_neutral=False,
    )

    assert refined["dels"] == []
    assert refined["dets"] == []


def test_refine_tag_assignment_strips_overall_fallback_when_specific_theme_exists():
    refined = refine_tag_assignment(
        "Great value for the price and it performs well every time.",
        detractors=[],
        delighters=["Overall Satisfaction", "Good Value", "Performs Well"],
        allowed_detractors=[],
        allowed_delighters=[],
        evidence_det={},
        evidence_del={},
    )

    assert "Good Value" in refined["dels"]
    assert "Performs Well" in refined["dels"]
    assert "Overall Satisfaction" not in refined["dels"]


def test_refine_tag_assignment_keeps_overall_fallback_when_no_specific_theme_exists():
    refined = refine_tag_assignment(
        "This is great and I am happy with it.",
        detractors=[],
        delighters=[],
        allowed_detractors=[],
        allowed_delighters=[],
        evidence_det={},
        evidence_del={},
    )

    assert refined["dels"] == ["Overall Satisfaction"]


def test_refine_tag_assignment_treats_no_issues_with_noise_as_quiet_not_loud():
    refined = refine_tag_assignment(
        "No issues with noise.",
        detractors=["Loud"],
        delighters=["Quiet"],
        allowed_detractors=["Loud"],
        allowed_delighters=["Quiet"],
        evidence_det={},
        evidence_del={},
    )

    assert refined["dets"] == []
    assert refined["dels"] == ["Quiet"]


def test_refine_tag_assignment_treats_not_as_quiet_as_expected_as_loud():
    refined = refine_tag_assignment(
        "Not as quiet as expected.",
        detractors=[],
        delighters=["Quiet"],
        allowed_detractors=["Loud"],
        allowed_delighters=["Quiet"],
        evidence_det={},
        evidence_del={},
    )

    assert refined["dets"] == ["Loud"]
    assert refined["dels"] == []


def test_refine_tag_assignment_treats_not_durable_as_unreliable_not_reliable():
    refined = refine_tag_assignment(
        "Great value, but not durable.",
        detractors=[],
        delighters=["Good Value", "Reliable"],
        allowed_detractors=["Unreliable"],
        allowed_delighters=["Good Value", "Reliable"],
        evidence_det={},
        evidence_del={},
    )

    assert "Unreliable" in refined["dets"]
    assert "Good Value" in refined["dels"]
    assert "Reliable" not in refined["dels"]


def test_refine_tag_assignment_does_not_bleed_easy_setup_into_easy_clean():
    refined = refine_tag_assignment(
        "Easy to use once you figure out the setup.",
        detractors=["Difficult Setup"],
        delighters=["Easy To Use", "Easy To Clean", "Easy Setup"],
        allowed_detractors=["Difficult Setup"],
        allowed_delighters=["Easy To Use", "Easy To Clean", "Easy Setup"],
        evidence_det={},
        evidence_del={},
    )

    assert refined["dets"] == ["Difficult Setup"]
    assert "Easy To Use" in refined["dels"]
    assert "Easy To Clean" not in refined["dels"]
    assert "Easy Setup" not in refined["dels"]



def test_normalize_tag_list_collapses_overlap_variants():
    labels = normalize_tag_list([
        "Excess Noise",
        "Loud",
        "Hard To Use",
        "Difficult To Use",
        "Easy Cleanup",
        "Easy To Clean",
    ])

    assert labels == ["Loud", "Difficult To Use", "Easy To Clean"]
