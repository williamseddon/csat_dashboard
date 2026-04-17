from __future__ import annotations

from pathlib import Path

import pandas as pd

from review_analyst.local_database import (
    count_local_review_db_selection,
    ensure_local_review_db_dirs,
    get_local_review_db_filter_options,
    get_local_review_db_status,
    load_local_review_workspace,
    load_local_review_analytics_frame,
    sync_local_review_database,
)
from review_analyst.normalization import finalize_df, normalize_uploaded_df



def _write_mapping(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "SKU": "HD436SLUK",
                "Master Item": "HD400",
                "Brand": "Shark",
                "Category": "Hair Care",
                "SubCategory": "Stylers",
                "SubsubCategory": "Air Stylers",
                "REGION": "EU/UK",
                "Item Status": "Active",
                "Lifecyle Phase": "Production Core",
                "Description": "FlexStyle 4-in-1",
            },
            {
                "SKU": "XSKWCOMB400EU",
                "Master Item": "HD400",
                "Brand": "Shark",
                "Category": "Hair Care",
                "SubCategory": "Accessories",
                "SubsubCategory": "Combs",
                "REGION": "EU/UK",
                "Item Status": "Active",
                "Lifecyle Phase": "Production Core",
                "Description": "Wide tooth comb",
            },
            {
                "SKU": "DB351UKCY",
                "Master Item": "DB300",
                "Brand": "Ninja",
                "Category": "Beverage",
                "SubCategory": "Blenders",
                "SubsubCategory": "Tumbler Blender",
                "REGION": "EU/UK",
                "Item Status": "Active",
                "Lifecyle Phase": "Production Core",
                "Description": "BlendBoss",
            },
        ]
    ).to_excel(path, index=False)



def _write_reviews(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "Region": "EU/UK",
                "Review ID": 1,
                "Review Submission Date": "2026-04-15 05:38:46",
                "Brand": "Shark",
                "Product ID": "HD436SLUK",
                "Base SKU": "HD400",
                "Review Title": "Do not buy",
                "Review Text": "It broke.",
                "Overall Rating": 1,
                "Review Display Locale": "en_GB",
                "Country": "UK",
                "Product Name": "Shark FlexStyle",
                "Product Page URL": "https://example.com/hd436",
                "Category Hierarchy": "Shark",
            },
            {
                "Region": "EU/UK",
                "Review ID": 2,
                "Review Submission Date": "2026-04-14 21:35:26",
                "Brand": "Shark",
                "Product ID": "XSKWCOMB400EU",
                "Base SKU": "HD400",
                "Review Title": "Great add-on",
                "Review Text": "Works well.",
                "Overall Rating": 5,
                "Review Display Locale": "de_DE",
                "Country": "DE",
                "Product Name": "Comb",
                "Product Page URL": "https://example.com/comb",
                "Category Hierarchy": "Shark",
            },
            {
                "Region": "EU/UK",
                "Review ID": 3,
                "Review Submission Date": "2026-04-13 10:00:00",
                "Brand": "Ninja",
                "Product ID": "DB351UKCY",
                "Base SKU": "DB300",
                "Review Title": "Nice blender",
                "Review Text": "Love it.",
                "Overall Rating": 4,
                "Review Display Locale": "en_GB",
                "Country": "UK",
                "Product Name": "BlendBoss",
                "Product Page URL": "https://example.com/db351",
                "Category Hierarchy": "Ninja",
            },
        ]
    ).to_csv(path, index=False)



def test_normalize_uploaded_df_prefers_product_id_and_parses_merged_review_aliases():
    raw = pd.DataFrame(
        [
            {
                "Review ID": 100,
                "Review Submission Date": "2026-04-15 05:38:46",
                "Product ID": "HD436SLUK",
                "Base SKU": "HD400",
                "Review Title": "Do not buy",
                "Review Text": "It broke.",
                "Overall Rating": 1,
                "Product Name": "Shark FlexStyle",
                "Country": "UK",
            }
        ]
    )
    normalized = normalize_uploaded_df(raw, source_name="merged_reviews.csv")
    normalized = finalize_df(normalized)

    assert normalized.loc[0, "product_id"] == "HD436SLUK"
    assert normalized.loc[0, "base_sku"] == "HD400"
    assert normalized.loc[0, "product_or_sku"] == "HD436SLUK"
    assert float(normalized.loc[0, "rating"]) == 1.0
    assert str(normalized.loc[0, "submission_date"]) == "2026-04-15"



def test_local_database_sync_builds_base_model_directory_and_loads_slice(tmp_path: Path):
    dirs = ensure_local_review_db_dirs(str(tmp_path))
    _write_mapping(Path(dirs["mapping_dir"]) / "master_mapping.xlsx")
    _write_reviews(Path(dirs["reviews_dir"]) / "merged_reviews.csv")

    before = get_local_review_db_status(str(tmp_path))
    assert before["needs_sync"] is True

    sync_result = sync_local_review_database(str(tmp_path), force=True)
    assert "Synced 3 reviews" in sync_result.get("message", "")

    after = get_local_review_db_status(str(tmp_path))
    assert after["is_ready"] is True
    assert after["needs_sync"] is False
    assert int(after["manifest"]["review_count"]) == 3
    assert int(after["manifest"]["base_model_count"]) == 2

    options = get_local_review_db_filter_options(str(tmp_path))
    assert "HD400" in options["base_model_number"]
    assert "Hair Care" in options["mapped_category"]

    count = count_local_review_db_selection(str(tmp_path), base_model_number="HD400")
    assert count == 2

    dataset = load_local_review_workspace(str(tmp_path), base_model_number="HD400", limit_rows=10)
    df = dataset["reviews_df"]
    assert dataset["source_type"] == "local_database"
    assert len(df) == 2
    assert set(df["base_model_number"].astype(str)) == {"HD400"}
    assert set(df["product_id"].astype(str)) == {"HD436SLUK", "XSKWCOMB400EU"}
    assert set(df["mapped_subcategory"].astype(str)) == {"Stylers", "Accessories"}
    assert set(df["catalog_match_type"].astype(str)) == {"product_to_sku"}



def test_normalize_uploaded_df_maps_incentivized_and_moderation_metadata():
    raw = pd.DataFrame(
        [
            {
                "Review ID": 101,
                "Review Submission Date": "2026-04-15 05:38:46",
                "Product ID": "HD436SLUK",
                "Base SKU": "HD400",
                "Review Title": "Seeded review",
                "Review Text": "Loved the styling attachments.",
                "Overall Rating": 5,
                "Product Name": "Shark FlexStyle",
                "Country": "UK",
                "IncentivizedReview (CDV)": "True",
                "Moderation Status": "SUBMITTED",
                "Campaign ID": "influenster_voxbox_api",
            }
        ]
    )
    normalized = finalize_df(normalize_uploaded_df(raw, source_name="merged_reviews.csv"))

    assert bool(normalized.loc[0, "incentivized_review"]) is True
    assert normalized.loc[0, "moderation_status"] == "SUBMITTED"
    assert normalized.loc[0, "moderation_bucket"] == "Pending"
    assert normalized.loc[0, "review_origin_group"] == "Seeded / Incentivized"
    assert normalized.loc[0, "review_acquisition_channel"] == "Influenster VoxBox"
    assert normalized.loc[0, "country"] == "UK"



def test_local_review_analytics_frame_respects_moderation_and_organic_filters(tmp_path: Path):
    dirs = ensure_local_review_db_dirs(str(tmp_path))
    _write_mapping(Path(dirs["mapping_dir"]) / "master_mapping.xlsx")
    pd.DataFrame(
        [
            {
                "Review ID": 1,
                "Review Submission Date": "2026-04-15 05:38:46",
                "Brand": "Shark",
                "Product ID": "HD436SLUK",
                "Base SKU": "HD400",
                "Review Title": "Seeded",
                "Review Text": "Sampled review",
                "Overall Rating": 5,
                "Country": "UK",
                "Product Name": "Shark FlexStyle",
                "Moderation Status": "APPROVED",
                "IncentivizedReview (CDV)": "True",
                "Campaign ID": "influenster_voxbox_api",
            },
            {
                "Review ID": 2,
                "Review Submission Date": "2026-04-14 05:38:46",
                "Brand": "Shark",
                "Product ID": "HD436SLUK",
                "Base SKU": "HD400",
                "Review Title": "Organic pending",
                "Review Text": "Waiting for moderation",
                "Overall Rating": 4,
                "Country": "UK",
                "Product Name": "Shark FlexStyle",
                "Moderation Status": "SUBMITTED",
                "IncentivizedReview (CDV)": "False",
                "Campaign ID": "BV_PIE_MPR",
            },
            {
                "Review ID": 3,
                "Review Submission Date": "2026-04-13 05:38:46",
                "Brand": "Shark",
                "Product ID": "HD436SLUK",
                "Base SKU": "HD400",
                "Review Title": "Rejected",
                "Review Text": "Should be hidden by default",
                "Overall Rating": 1,
                "Country": "UK",
                "Product Name": "Shark FlexStyle",
                "Moderation Status": "REJECTED",
                "IncentivizedReview (CDV)": "False",
                "Campaign ID": "BV_PIE_MPR",
            },
        ]
    ).to_csv(Path(dirs["reviews_dir"]) / "merged_reviews.csv", index=False)

    sync_local_review_database(str(tmp_path), force=True)

    approved_pending = load_local_review_analytics_frame(
        str(tmp_path),
        moderation_buckets=["Approved", "Pending"],
    )
    assert len(approved_pending) == 2
    assert set(approved_pending["moderation_bucket"].astype(str)) == {"Approved", "Pending"}

    organic_only = load_local_review_analytics_frame(
        str(tmp_path),
        moderation_buckets=["Approved", "Pending", "Rejected"],
        organic_only=True,
    )
    assert len(organic_only) == 2
    assert organic_only["incentivized_review"].sum() == 0
    assert set(organic_only["review_origin_group"].astype(str)) == {"Organic"}
