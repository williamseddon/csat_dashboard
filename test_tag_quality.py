from __future__ import annotations

import importlib
import io
import sys
import types

import pandas as pd
import pytest


class _StreamlitStub(types.ModuleType):
    def __init__(self) -> None:
        super().__init__("streamlit")
        self.session_state: dict[str, object] = {}

    def cache_data(self, *args, **kwargs):
        def _decorator(fn):
            fn.clear = lambda: None
            return fn

        if args and callable(args[0]) and len(args) == 1 and not kwargs:
            return _decorator(args[0])
        return _decorator

    cache_resource = cache_data

    def set_page_config(self, *args, **kwargs):
        return None

    def markdown(self, *args, **kwargs):
        return None

    def __getattr__(self, name: str):
        def _noop(*args, **kwargs):
            return None

        return _noop


@pytest.fixture()
def app_module(monkeypatch):
    monkeypatch.setitem(sys.modules, "streamlit", _StreamlitStub())
    sys.modules.pop("app", None)
    return importlib.import_module("app")


def test_core_filters_and_extra_filters_follow_workspace_schema(app_module):
    df = pd.DataFrame(
        {
            "Merchant": ["Amazon", "Target", "Amazon"],
            "Locale": ["en_US", "en_GB", "en_US"],
            "Custom Segment": ["Premium", "Mass", "Premium"],
            "Review Body": ["great value " * 20, "solid product " * 18, "love it " * 15],
            "Review GUID": ["a1", "a2", "a3"],
            "Review Date": ["2026-04-01", "2026-04-04", "2026-04-10"],
            "rating": [5, 4, 3],
        }
    )

    specs = app_module._core_filter_specs_for_df(df)
    resolved_columns = {spec["label"]: spec.get("column") for spec in specs if spec.get("kind") == "column"}
    assert resolved_columns["Retailer"] == "Merchant"
    assert resolved_columns["Market / Locale"] == "Locale"

    extra_candidates = app_module._extra_filter_candidates(df)
    assert "Custom Segment" in extra_candidates
    assert "Review Date" in extra_candidates
    assert "Review Body" in extra_candidates
    assert "Review GUID" in extra_candidates
    assert app_module._infer_extra_filter_kind(df, "Review Body") == "text"
    assert app_module._infer_extra_filter_kind(df, "Review GUID") == "text"


def test_clean_watch_dimension_series_preserves_display_case_and_cleans_null_tokens(app_module):
    cleaned = app_module._clean_watch_dimension_series(pd.Series(["Amazon", " undefined ", None, "DTC"]), unknown="Unknown")

    assert cleaned.tolist() == ["Amazon", "Unknown", "Unknown", "DTC"]



def test_sw_style_fig_moves_dense_legends_vertical(app_module):
    import plotly.graph_objects as go

    fig = go.Figure()
    fig.add_bar(name="Unknown region", x=["Amazon"], y=[3.8])
    fig.add_bar(name="UK", x=["Amazon"], y=[4.1])
    fig.add_bar(name="USA", x=["Amazon"], y=[4.4])

    styled = app_module._sw_style_fig(fig)

    assert styled.layout.legend.orientation == "v"



def test_retailer_watch_uses_selected_retailer_region_and_date_columns(app_module):
    df = pd.DataFrame(
        {
            "Merchant": ["Amazon", "Amazon", "Target", "Target"],
            "Market": ["United States", "United Kingdom", "United States", "United Kingdom"],
            "Review Date": ["2026-03-25", "2026-04-05", "2026-04-10", "2026-04-12"],
            "Sponsored": [False, True, False, False],
            "rating": [5, 2, 4, 3],
        }
    )

    table_df, region_kpis_df, known_regions = app_module._prepare_source_rating_watch(
        df,
        organic_only=True,
        selected_regions=["USA", "UK"],
        combine_regions=False,
        source_col="Merchant",
        region_col="Market",
        date_col="Review Date",
    )

    assert set(known_regions) == {"UK", "USA"}
    assert table_df.columns.tolist().count("Retailer") == 1
    assert set(table_df["Retailer"]) == {"Amazon", "Target"}
    assert set(table_df["Region"]) == {"UK", "USA"}

    target_uk_reviews = table_df.loc[(table_df["Retailer"] == "Target") & (table_df["Region"] == "UK"), "Reviews"].iloc[0]
    assert int(target_uk_reviews) == 1

    amazon_uk_rows = table_df.loc[(table_df["Retailer"] == "Amazon") & (table_df["Region"] == "UK")]
    assert amazon_uk_rows.empty, "Organic-only mode should drop the sponsored Amazon UK row."

    all_selected_row = region_kpis_df.iloc[0]
    assert all_selected_row["Region"] == "All selected"
    assert int(all_selected_row["reviews"]) == 3
    assert float(all_selected_row["avg_rating"]) == pytest.approx(4.0, abs=1e-6)


def test_retailer_watch_auto_detects_axion_like_columns_and_seeded_filter(app_module):
    df = pd.DataFrame(
        {
            "Retailer": ["Sephora", "Sephora", "Amazon"],
            "Location": ["United States", "United Kingdom", "United States"],
            "Opened date": ["2026-04-01", "2026-04-04", "2026-04-10"],
            "Seeded Flag": ["Not Seeded", "Seeded", "Not Seeded"],
            "rating": [5, 2, 4],
        }
    )

    assert app_module._resolve_column_alias(df, app_module.WATCH_SOURCE_COLUMN_ALIASES) == "Retailer"
    assert app_module._resolve_column_alias(df, app_module.WATCH_REGION_COLUMN_ALIASES) == "Location"
    assert app_module._resolve_column_alias(df, app_module.WATCH_DATE_COLUMN_ALIASES) == "Opened date"
    assert app_module._resolve_column_alias(df, app_module.WATCH_ORGANIC_COLUMN_ALIASES) == "Seeded Flag"

    table_df, region_kpis_df, known_regions = app_module._prepare_source_rating_watch(
        df,
        organic_only=True,
        selected_regions=["USA", "UK"],
        combine_regions=False,
    )

    assert set(known_regions) == {"USA"}
    assert table_df.columns.tolist().count("Retailer") == 1
    assert set(table_df["Retailer"]) == {"Amazon", "Sephora"}
    assert set(table_df["Region"]) == {"USA"}
    assert int(region_kpis_df.iloc[0]["reviews"]) == 2


def test_uploaded_source_column_normalizes_into_retailer_for_watch(app_module):
    raw = pd.DataFrame(
        {
            "Source": ["Best Buy", "DTC"],
            "Country": ["United States", "United Kingdom"],
            "Review Date": ["2026-04-01", "2026-04-02"],
            "Star Rating": [5, 4],
            "Review": ["Excellent", "Solid"],
        }
    )

    normalized = app_module._normalize_uploaded_df(raw, source_name="sample.xlsx")
    assert set(normalized["retailer"].astype("string")) == {"Best Buy", "DTC"}

    table_df, _, known_regions = app_module._prepare_source_rating_watch(
        normalized,
        organic_only=False,
        combine_regions=False,
    )

    assert set(known_regions) == {"UK", "USA"}
    assert set(table_df["Retailer"]) == {"Best Buy", "DTC"}



def test_retailer_watch_payload_surfaces_alerts_trends_and_symptoms(app_module):
    df = pd.DataFrame(
        {
            "Retailer": ["Amazon", "Amazon", "Amazon", "Amazon", "Amazon", "Target", "Target"],
            "Reviewer Location": ["United States", "United States", "United States", "United States", "United States", "United States", "United States"],
            "Review Date": ["2026-02-10", "2026-02-24", "2026-04-02", "2026-04-06", "2026-04-08", "2026-03-15", "2026-04-10"],
            "rating": [5, 5, 2, 2, 2, 4, 4],
            "AI Symptom Detractor 1": [None, None, "Battery Problem", "Battery Problem", "Battery Problem", None, None],
            "AI Symptom Delighter 1": ["Easy To Use", "Easy To Use", None, None, None, "Easy To Use", "Easy To Use"],
        }
    )

    payload = app_module._prepare_source_rating_watch_payload(
        df,
        organic_only=False,
        selected_regions=["USA"],
        combine_regions=False,
        source_col="Retailer",
        region_col="Reviewer Location",
        date_col="Review Date",
    )

    alerts_df = payload["alerts_df"]
    trend_df = payload["trend_df"]
    symptom_summary_df = payload["symptom_summary_df"]

    assert not alerts_df.empty
    assert "Amazon" in set(alerts_df["Retailer"])
    assert not trend_df.empty
    assert {"Week", "Retailer", "Avg Rating", "Reviews"}.issubset(set(trend_df.columns))
    assert not symptom_summary_df.empty
    assert "Battery Problem" in set(symptom_summary_df["Label"])



def test_prepare_source_rating_watch_handles_duplicate_retailer_columns(app_module):
    df = pd.DataFrame(
        [
            ["Amazon", "Amazon", "United States", "2026-04-01", 5],
            ["Target", "Target", "United Kingdom", "2026-04-02", 4],
        ],
        columns=["Retailer", "Retailer", "Reviewer Location", "Opened date", "rating"],
    )

    table_df, _, known_regions = app_module._prepare_source_rating_watch(
        df,
        organic_only=False,
        selected_regions=["USA", "UK"],
        combine_regions=False,
        source_col="Retailer",
        region_col="Reviewer Location",
        date_col="Opened date",
    )

    assert table_df.columns.tolist().count("Retailer") == 1
    assert set(known_regions) == {"UK", "USA"}
    assert set(table_df["Retailer"]) == {"Amazon", "Target"}



def test_uploaded_normalization_prefers_retailer_over_source(app_module):
    raw = pd.DataFrame(
        {
            "Retailer": ["Amazon", "Target"],
            "Source": ["PowerReviews", "PowerReviews"],
            "Reviewer Location": ["United States", "United Kingdom"],
            "Review Date": ["2026-04-01", "2026-04-02"],
            "Star Rating": [5, 4],
            "Review": ["Excellent", "Solid"],
        }
    )

    normalized = app_module._normalize_uploaded_df(raw, source_name="sample.xlsx")

    assert set(normalized["retailer"].astype("string")) == {"Amazon", "Target"}
    assert set(normalized["user_location"].astype("string")) == {"United States", "United Kingdom"}



def test_best_uploaded_excel_sheet_prefers_review_like_sheet(app_module):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        pd.DataFrame({"Detractors": ["Noisy"], "Delighters": ["Easy To Use"]}).to_excel(writer, sheet_name="Symptoms", index=False)
        pd.DataFrame(
            {
                "Review Text": ["Works well", "Too loud"],
                "Rating (num)": [5, 2],
                "Retailer": ["Amazon", "Amazon"],
                "Opened date": ["2026-04-01", "2026-04-02"],
            }
        ).to_excel(writer, sheet_name="Reviews", index=False)
    bio.seek(0)

    df, sheet_name = app_module._read_best_uploaded_excel_sheet(bio.getvalue())

    assert sheet_name == "Reviews"
    assert list(df.columns) == ["Review Text", "Rating (num)", "Retailer", "Opened date"]



def test_uploaded_normalization_maps_verbatim_style_review_fields(app_module):
    raw = pd.DataFrame(
        {
            "Verbatim Id": [12345],
            "Model (SKU)": ["HD600"],
            "Source": ["Best Buy"],
            "Country": ["USA"],
            "Review Date": ["2026-04-01"],
            "Review title": ["Great dryer"],
            "Verbatim": ["Loved it and use it every day."],
            "Star Rating": [5],
            "Web Link": ["https://example.com/review"],
        }
    )

    normalized = app_module._normalize_uploaded_df(raw, source_name="starwalk.xlsx", include_local_symptomization=True)

    assert str(normalized.loc[0, "review_id"]) == "12345"
    assert normalized.loc[0, "product_id"] == "HD600"
    assert normalized.loc[0, "base_sku"] == "HD600"
    assert normalized.loc[0, "title"] == "Great dryer"
    assert normalized.loc[0, "review_text"] == "Loved it and use it every day."
    assert normalized.loc[0, "post_link"] == "https://example.com/review"


def test_extra_filters_can_surface_uploaded_text_and_id_headers(app_module):
    df = pd.DataFrame(
        {
            "Merchant": ["Amazon", "Target", "Amazon"],
            "Locale": ["en_US", "en_GB", "en_US"],
            "Review Body": ["great value " * 20, "solid product " * 18, "love it " * 15],
            "Review GUID": ["a1", "a2", "a3"],
            "Review Date": ["2026-04-01", "2026-04-04", "2026-04-10"],
            "Custom Segment": ["Premium", "Mass", "Premium"],
            "rating": [5, 4, 3],
        }
    )

    extra_candidates = app_module._extra_filter_candidates(df)
    assert "Custom Segment" in extra_candidates
    assert "Review Date" in extra_candidates
    assert "Review Body" in extra_candidates
    assert "Review GUID" in extra_candidates
    assert app_module._infer_extra_filter_kind(df, "Review Body") == "text"
    assert app_module._infer_extra_filter_kind(df, "Review GUID") == "text"
    assert app_module._infer_extra_filter_kind(df, "Review Date") == "date"
    assert app_module._infer_extra_filter_kind(df, "Custom Segment") == "categorical"


def test_uploaded_normalization_preserves_original_headers_for_filter_builder(app_module):
    raw = pd.DataFrame(
        {
            "Retailer": ["Amazon", "Target"],
            "Campaign Name": ["Spring Promo", "Always On"],
            "Reviewer Notes": ["Mentions loud motor", "Mentions easy setup"],
            "Case ID": ["C-100", "C-101"],
            "Review Date": ["2026-04-01", "2026-04-02"],
            "Star Rating": [2, 5],
            "Review": ["Too loud", "Very easy"],
        }
    )

    normalized = app_module._normalize_uploaded_df(raw, source_name="sample.xlsx")
    extra_candidates = app_module._extra_filter_candidates(normalized)

    assert "Campaign Name" in normalized.columns
    assert "Reviewer Notes" in normalized.columns
    assert "Case ID" in normalized.columns
    assert "Campaign Name" in extra_candidates
    assert "Reviewer Notes" in extra_candidates
    assert "Case ID" in extra_candidates
    assert app_module._infer_extra_filter_kind(normalized, "Reviewer Notes") in {"text", "categorical"}
    assert app_module._infer_extra_filter_kind(normalized, "Case ID") in {"text", "categorical"}


def test_retailer_watch_is_wrapped_in_closed_expander(app_module, monkeypatch):
    calls = []

    class _Ctx:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(app_module.st, "expander", lambda label, expanded=False, **kwargs: calls.append((label, expanded)) or _Ctx())
    monkeypatch.setattr(app_module.st, "container", lambda *args, **kwargs: _Ctx())

    app_module._render_source_rating_watch(pd.DataFrame())

    assert calls[0] == ("🏪 Retailer rating watch", False)
