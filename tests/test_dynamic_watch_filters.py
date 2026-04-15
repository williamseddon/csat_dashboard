from __future__ import annotations

import importlib
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
    assert "Review Body" not in extra_candidates
    assert "Review GUID" not in extra_candidates


def test_retailer_watch_uses_selected_source_region_and_date_columns(app_module):
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
    assert set(table_df["Source"]) == {"Amazon", "Target"}
    assert set(table_df["Region"]) == {"UK", "USA"}

    target_uk_reviews = table_df.loc[(table_df["Source"] == "Target") & (table_df["Region"] == "UK"), "Reviews"].iloc[0]
    assert int(target_uk_reviews) == 1

    amazon_uk_rows = table_df.loc[(table_df["Source"] == "Amazon") & (table_df["Region"] == "UK")]
    assert amazon_uk_rows.empty, "Organic-only mode should drop the sponsored Amazon UK row."

    all_selected_row = region_kpis_df.iloc[0]
    assert all_selected_row["Region"] == "All selected"
    assert int(all_selected_row["reviews"]) == 3
    assert float(all_selected_row["avg_rating"]) == pytest.approx(4.0, abs=1e-6)
