from __future__ import annotations

import io

import pandas as pd

from .analytics import compute_metrics, monthly_trend, rating_distribution


def build_master_excel(summary, reviews_df: pd.DataFrame) -> bytes:
    metrics = compute_metrics(reviews_df)
    rating_df = rating_distribution(reviews_df)
    trend_df = monthly_trend(reviews_df)
    summary_df = pd.DataFrame(
        [
            {
                'product_id': summary.product_id,
                'product_url': summary.product_url,
                'reviews_downloaded': summary.reviews_downloaded,
                'avg_rating': metrics.get('avg_rating'),
                'avg_rating_non_incentivized': metrics.get('avg_rating_non_incentivized'),
                'pct_low_star': metrics.get('pct_low_star'),
                'pct_incentivized': metrics.get('pct_incentivized'),
                'generated_utc': pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'),
            }
        ]
    )

    priority_cols = [
        'review_id', 'product_id', 'product_or_sku', 'rating', 'incentivized_review',
        'is_recommended', 'submission_time', 'content_locale', 'retailer', 'title', 'review_text',
    ]
    ordered = [col for col in priority_cols if col in reviews_df.columns]
    remaining = [col for col in reviews_df.columns if col not in ordered]
    export_reviews = reviews_df[ordered + remaining]

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as writer:
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        export_reviews.to_excel(writer, sheet_name='Reviews', index=False)
        rating_df.to_excel(writer, sheet_name='RatingDistribution', index=False)
        trend_df.to_excel(writer, sheet_name='ReviewVolume', index=False)

        for sheet_name, frame in {
            'Summary': summary_df,
            'Reviews': export_reviews,
            'RatingDistribution': rating_df,
            'ReviewVolume': trend_df,
        }.items():
            ws = writer.sheets[sheet_name]
            ws.freeze_panes = 'A2'
            for idx, col in enumerate(frame.columns, 1):
                series = frame[col].head(250).fillna('').astype(str)
                max_len = max([len(str(col))] + [len(value) for value in series.tolist()])
                ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = min(max_len + 2, 48)

    out.seek(0)
    return out.getvalue()
