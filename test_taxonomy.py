import warnings

import pandas as pd
import pytest

from review_analyst.symptoms import add_net_hit, analyze_symptoms_fast


def test_analyze_symptoms_fast_dedupes_duplicate_tags_within_review():
    df = pd.DataFrame(
        {
            "rating": [1, 3, 5],
            "AI Symptom Detractor 1": ["Noise", "Noise", "Not Mentioned"],
            "AI Symptom Detractor 2": ["Noise", None, "<NA>"],
        }
    )

    tbl = analyze_symptoms_fast(df, ["AI Symptom Detractor 1", "AI Symptom Detractor 2"])

    assert list(tbl["Item"]) == ["Noise"]
    assert int(tbl.loc[0, "Mentions"]) == 2
    assert float(tbl.loc[0, "Avg Star"]) == pytest.approx(2.0)
    assert float(tbl.loc[0, "Avg Tags/Review"]) == pytest.approx(1.0)
    assert tbl.loc[0, "% Tagged Reviews"] == "100.0%"


def test_analyze_symptoms_fast_percent_uses_only_symptomized_reviews():
    df = pd.DataFrame(
        {
            "rating": [1, 2, 4, 5],
            "AI Symptom Detractor 1": ["Noise", None, "Battery", None],
            "AI Symptom Detractor 2": [None, None, None, None],
        }
    )

    tbl = analyze_symptoms_fast(df, ["AI Symptom Detractor 1", "AI Symptom Detractor 2"])
    by_item = tbl.set_index("Item")

    assert by_item.loc["Noise", "% Tagged Reviews"] == "50.0%"
    assert by_item.loc["Battery", "% Tagged Reviews"] == "50.0%"


def test_add_net_hit_allocates_review_gap_across_multiple_tags():
    df = pd.DataFrame(
        {
            "rating": [1, 1, 4],
            "AI Symptom Detractor 1": ["Noise", "Noise", "Price"],
            "AI Symptom Detractor 2": ["Price", None, None],
        }
    )
    cols = ["AI Symptom Detractor 1", "AI Symptom Detractor 2"]
    tbl = analyze_symptoms_fast(df, cols)

    out = add_net_hit(tbl, avg_rating=4.0, total_reviews=len(df), kind="detractors", shrink_k=3.0, detail_df=df, symptom_cols=cols)
    by_item = out.set_index("Item")

    assert by_item.loc["Noise", "% Tagged Reviews"] == "66.7%"
    assert by_item.loc["Price", "% Tagged Reviews"] == "66.7%"
    assert float(by_item.loc["Noise", "Avg Tags/Review"]) == pytest.approx(1.5)
    assert float(by_item.loc["Price", "Avg Tags/Review"]) == pytest.approx(1.5)
    assert float(by_item.loc["Noise", "Net Hit"]) == pytest.approx(-1.500, rel=1e-6)
    assert float(by_item.loc["Noise", "Forecast Δ★"]) == pytest.approx(-0.671, rel=1e-3)
    assert float(by_item.loc["Price", "Net Hit"]) == pytest.approx(-0.500, rel=1e-6)
    assert float(by_item.loc["Price", "Forecast Δ★"]) == pytest.approx(-0.224, rel=1e-3)
    assert abs(float(by_item.loc["Noise", "Forecast Δ★"])) > abs(float(by_item.loc["Price", "Forecast Δ★"]))


def test_analyze_symptoms_fast_uses_row_positions_and_avoids_stack_warning():
    df = pd.DataFrame(
        {
            "rating": [1, 3, 5],
            "AI Symptom Detractor 1": ["Noise", "Noise", "Not Mentioned"],
            "AI Symptom Detractor 2": ["Noise", None, "<NA>"],
        }
    )
    df.index = [7, 7, 9]

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        tbl = analyze_symptoms_fast(df, ["AI Symptom Detractor 1", "AI Symptom Detractor 2"])

    assert list(tbl["Item"]) == ["Noise"]
    assert int(tbl.loc[0, "Mentions"]) == 2
    assert float(tbl.loc[0, "Avg Star"]) == pytest.approx(2.0)
    assert tbl.loc[0, "% Tagged Reviews"] == "100.0%"



def test_add_net_hit_exposes_confidence_and_impact_score_columns():
    df = pd.DataFrame(
        {
            "rating": [1, 2, 5],
            "AI Symptom Detractor 1": ["Broken", "Broken", None],
            "AI Symptom Detractor 2": [None, None, None],
        }
    )
    cols = ["AI Symptom Detractor 1", "AI Symptom Detractor 2"]
    tbl = analyze_symptoms_fast(df, cols)

    out = add_net_hit(tbl, avg_rating=4.0, total_reviews=len(df), kind="detractors", shrink_k=3.0, detail_df=df, symptom_cols=cols)
    row = out.iloc[0]

    assert "Confidence %" in out.columns
    assert "Severity Wt" in out.columns
    assert "Impact Score" in out.columns
    assert float(row["Net Hit"]) < 0
    assert float(row["Forecast Δ★"]) < 0
    assert float(row["Impact Score"]) < 0
    assert float(row["Severity Wt"]) > 1.0
    assert 0.0 <= float(row["Confidence %"]) <= 100.0


def test_add_net_hit_gives_delighters_positive_impact_score():
    df = pd.DataFrame(
        {
            "rating": [5, 5, 3],
            "AI Symptom Delighter 1": ["Reliable", "Reliable", None],
            "AI Symptom Delighter 2": [None, None, None],
        }
    )
    cols = ["AI Symptom Delighter 1", "AI Symptom Delighter 2"]
    tbl = analyze_symptoms_fast(df, cols)

    out = add_net_hit(tbl, avg_rating=4.0, total_reviews=len(df), kind="delighters", shrink_k=3.0, detail_df=df, symptom_cols=cols)
    row = out.iloc[0]

    assert float(row["Net Hit"]) > 0
    assert float(row["Forecast Δ★"]) > 0
    assert float(row["Impact Score"]) > 0
    assert float(row["Severity Wt"]) >= 1.0


def test_analyze_symptoms_fast_excludes_overall_fallback_from_main_rows_but_preserves_denominator():
    df = pd.DataFrame(
        {
            "rating": [1, 2],
            "AI Symptom Detractor 1": ["Overall Dissatisfaction", "Noise"],
            "AI Symptom Detractor 2": [None, "Overall Dissatisfaction"],
        }
    )

    tbl = analyze_symptoms_fast(df, ["AI Symptom Detractor 1", "AI Symptom Detractor 2"])

    assert list(tbl["Item"]) == ["Noise"]
    assert tbl.loc[0, "% Tagged Reviews"] == "50.0%"


def test_add_net_hit_excludes_overall_fallback_from_impact_math():
    df = pd.DataFrame(
        {
            "rating": [1, 1, 5],
            "AI Symptom Detractor 1": ["Overall Dissatisfaction", "Noise", None],
            "AI Symptom Detractor 2": [None, "Overall Dissatisfaction", None],
        }
    )
    cols = ["AI Symptom Detractor 1", "AI Symptom Detractor 2"]
    tbl = analyze_symptoms_fast(df, cols)
    out = add_net_hit(tbl, avg_rating=4.0, total_reviews=len(df), kind="detractors", shrink_k=3.0, detail_df=df, symptom_cols=cols)

    assert list(out["Item"]) == ["Noise"]
    assert float(out.loc[0, "Net Hit"]) < 0



def test_symptom_tables_collapse_overlap_variants():
    df = pd.DataFrame({
        "rating": [2, 2, 4],
        "AI Symptom Detractor 1": ["Excess Noise", "Loud", pd.NA],
        "AI Symptom Detractor 2": [pd.NA, pd.NA, pd.NA],
        "AI Symptom Delighter 1": [pd.NA, pd.NA, "Easy Cleanup"],
    })

    tbl = analyze_symptoms_fast(df, ["AI Symptom Detractor 1", "AI Symptom Delighter 1"])
    items = tbl["Item"].tolist()

    assert "Loud" in items
    assert "Excess Noise" not in items
    assert "Easy To Clean" in items
