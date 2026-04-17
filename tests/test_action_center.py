from __future__ import annotations

import pandas as pd

from review_analyst.action_center import (
    detect_trend_movers,
    prepare_action_frame,
    summarize_base_models,
    summarize_dimension,
)



def _make_rows(brand: str, category: str, base_model: str, month: str, rating: float, n: int) -> list[dict]:
    rows = []
    for idx in range(n):
        rows.append(
            {
                "review_id": f"{brand}-{category}-{month}-{idx}",
                "submission_time": pd.Timestamp(month) + pd.Timedelta(days=idx % 25),
                "rating": rating,
                "incentivized_review": idx % 7 == 0,
                "moderation_bucket": "Approved",
                "mapped_brand": brand,
                "mapped_category": category,
                "mapped_subcategory": f"{category} Sub",
                "base_model_number": base_model,
                "product_id": f"{base_model}-{idx % 3}",
                "original_product_name": f"{brand} {base_model}",
                "country": "US" if idx % 2 == 0 else "CA",
            }
        )
    return rows



def test_prepare_action_frame_and_dimension_segments():
    rows = []
    rows += _make_rows("Shark", "Vacuums", "HD400", "2026-01-01", 4.2, 60)
    rows += _make_rows("Shark", "Vacuums", "HD400", "2026-02-01", 3.4, 60)
    rows += _make_rows("Shark", "Vacuums", "HD400", "2026-03-01", 3.1, 60)
    rows += _make_rows("Ninja", "Blenders", "DB300", "2026-01-01", 4.6, 60)
    rows += _make_rows("Ninja", "Blenders", "DB300", "2026-02-01", 4.7, 60)
    rows += _make_rows("Ninja", "Blenders", "DB300", "2026-03-01", 4.8, 60)
    df = prepare_action_frame(pd.DataFrame(rows))

    summary, trend = summarize_dimension(df, group_cols=["mapped_brand", "mapped_category"], freq="M", min_reviews=20)
    assert not summary.empty
    assert not trend.empty

    shark_row = summary[(summary["mapped_brand"] == "Shark") & (summary["mapped_category"] == "Vacuums")].iloc[0]
    ninja_row = summary[(summary["mapped_brand"] == "Ninja") & (summary["mapped_category"] == "Blenders")].iloc[0]

    assert shark_row["segment"] == "Fix now"
    assert float(shark_row["rating_delta"]) < 0
    assert ninja_row["segment"] == "Scale"
    assert float(ninja_row["rating_delta"]) > 0

    base_summary, _ = summarize_base_models(df, min_reviews=20, freq="M")
    assert set(base_summary["base_model_number"].astype(str)) == {"HD400", "DB300"}



def test_detect_trend_movers_surfaces_biggest_swings():
    rows = []
    rows += _make_rows("Shark", "Vacuums", "HD400", "2026-01-01", 4.2, 50)
    rows += _make_rows("Shark", "Vacuums", "HD400", "2026-02-01", 3.2, 50)
    rows += _make_rows("Shark", "Vacuums", "HD400", "2026-03-01", 3.0, 50)
    rows += _make_rows("Ninja", "Blenders", "DB300", "2026-01-01", 4.5, 50)
    rows += _make_rows("Ninja", "Blenders", "DB300", "2026-02-01", 4.7, 50)
    rows += _make_rows("Ninja", "Blenders", "DB300", "2026-03-01", 4.9, 50)
    df = prepare_action_frame(pd.DataFrame(rows))

    trend_df, movers = detect_trend_movers(df, group_col="mapped_category", metric="avg_rating", freq="M", min_group_reviews=20)
    assert not trend_df.empty
    assert not movers.empty

    vacuum = movers[movers["entity_label"] == "Vacuums"].iloc[0]
    blender = movers[movers["entity_label"] == "Blenders"].iloc[0]

    assert float(vacuum["delta"]) < 0
    assert float(blender["delta"]) > 0
