from __future__ import annotations

import re
from typing import Any, Dict, List, Sequence, Tuple

import numpy as np
import pandas as pd

from .utils import NON_VALUES, safe_mean, safe_pct, tokenize
from .symptoms import add_net_hit as symptom_add_net_hit


REGION_NAME_MAP = {
    'US': 'USA',
    'USA': 'USA',
    'GB': 'UK',
    'UK': 'UK',
    'CA': 'Canada',
    'AU': 'Australia',
    'DE': 'Germany',
    'FR': 'France',
    'ES': 'Spain',
    'IT': 'Italy',
    'JP': 'Japan',
    'MX': 'Mexico',
    'BR': 'Brazil',
    'NL': 'Netherlands',
}


def compute_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    count = len(df)
    if count == 0:
        return {
            'review_count': 0,
            'avg_rating': None,
            'avg_rating_non_incentivized': None,
            'pct_low_star': 0.0,
            'pct_one_star': 0.0,
            'pct_two_star': 0.0,
            'pct_five_star': 0.0,
            'pct_incentivized': 0.0,
            'pct_with_photos': 0.0,
            'pct_syndicated': 0.0,
            'recommend_rate': None,
            'median_review_words': None,
            'non_incentivized_count': 0,
            'low_star_count': 0,
        }

    working = df.copy()
    organic = working[~working['incentivized_review'].fillna(False)]
    recommend_base = working[working['is_recommended'].notna()]
    recommend_rate = safe_pct(int(recommend_base['is_recommended'].astype(bool).sum()), len(recommend_base)) if not recommend_base.empty else None
    median_words = float(working['review_length_words'].median()) if 'review_length_words' in working.columns and not working['review_length_words'].dropna().empty else None
    low = pd.to_numeric(working['rating'], errors='coerce') < 3
    return {
        'review_count': count,
        'avg_rating': safe_mean(working['rating']),
        'avg_rating_non_incentivized': safe_mean(organic['rating']),
        'pct_low_star': safe_pct(int(low.sum()), count),
        'pct_one_star': safe_pct(int((working['rating'] == 1).sum()), count),
        'pct_two_star': safe_pct(int((working['rating'] == 2).sum()), count),
        'pct_five_star': safe_pct(int((working['rating'] == 5).sum()), count),
        'pct_incentivized': safe_pct(int(working['incentivized_review'].fillna(False).sum()), count),
        'pct_with_photos': safe_pct(int(working['has_photos'].fillna(False).sum()), count),
        'pct_syndicated': safe_pct(int(working['is_syndicated'].fillna(False).sum()), count),
        'recommend_rate': recommend_rate,
        'median_review_words': median_words,
        'non_incentivized_count': len(organic),
        'low_star_count': int(low.sum()),
    }


def rating_distribution(df: pd.DataFrame) -> pd.DataFrame:
    base = pd.DataFrame({'rating': [1, 2, 3, 4, 5]})
    if df.empty:
        base['review_count'] = 0
        base['share'] = 0.0
        return base
    grouped = (
        df.dropna(subset=['rating'])
        # Round to nearest integer BEFORE casting — handles 3.5 → 4, not 3
        .assign(rating=lambda frame: frame['rating'].round().clip(1, 5).astype(int))
        .groupby('rating', as_index=False)
        .size()
        .rename(columns={'size': 'review_count'})
    )
    merged = base.merge(grouped, how='left', on='rating').fillna({'review_count': 0})
    merged['review_count'] = merged['review_count'].astype(int)
    merged['share'] = merged['review_count'] / max(len(df), 1)
    return merged


def monthly_trend(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=['submission_month', 'review_count', 'avg_rating', 'month_start'])
    working = df.copy()
    working['submission_time'] = pd.to_datetime(working.get('submission_time'), errors='coerce')
    return (
        working.dropna(subset=['submission_time'])
        .assign(month_start=lambda frame: frame['submission_time'].dt.to_period('M').dt.to_timestamp())
        .groupby('month_start', as_index=False)
        .agg(review_count=('review_id', 'count'), avg_rating=('rating', 'mean'))
        .assign(submission_month=lambda frame: frame['month_start'].dt.strftime('%Y-%m'))
        .sort_values('month_start')
    )


def cohort_by_incentivized(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    working = df.copy()
    working['cohort'] = working['incentivized_review'].fillna(False).map({True: 'Incentivized', False: 'Organic'})
    working['rating_int'] = pd.to_numeric(working['rating'], errors='coerce')
    working = working.dropna(subset=['rating_int'])
    working['rating_int'] = working['rating_int'].astype(int)
    rows: List[Dict[str, Any]] = []
    for cohort, group in working.groupby('cohort'):
        total = max(len(group), 1)
        for star in [1, 2, 3, 4, 5]:
            count = int((group['rating_int'] == star).sum())
            rows.append({'cohort': cohort, 'star': star, 'count': count, 'pct': count / total * 100})
    return pd.DataFrame(rows)


def locale_breakdown(df: pd.DataFrame, top_n: int = 12) -> pd.DataFrame:
    if df.empty or 'content_locale' not in df.columns:
        return pd.DataFrame()
    grouped = (
        df.dropna(subset=['content_locale'])
        .groupby('content_locale', as_index=False)
        .agg(count=('review_id', 'count'), avg_rating=('rating', 'mean'))
        .sort_values('count', ascending=False)
        .head(top_n)
    )
    grouped['pct'] = grouped['count'] / max(grouped['count'].sum(), 1) * 100
    return grouped


def review_length_cohort(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or 'review_length_words' not in df.columns:
        return pd.DataFrame()
    working = df.dropna(subset=['rating', 'review_length_words']).copy()
    working['review_length_words'] = pd.to_numeric(working['review_length_words'], errors='coerce')
    working = working.dropna(subset=['review_length_words'])
    if len(working) < 8:
        return pd.DataFrame()
    try:
        working['length_bin'] = pd.qcut(
            working['review_length_words'],
            q=4,
            labels=['Short (Q1)', 'Medium (Q2)', 'Long (Q3)', 'Very Long (Q4)'],
            duplicates='drop',
        )
    except Exception:
        return pd.DataFrame()
    return (
        working.groupby('length_bin', as_index=False, observed=True)
        .agg(avg_rating=('rating', 'mean'), count=('review_id', 'count'), median_words=('review_length_words', 'median'))
        .rename(columns={'length_bin': 'Length Quartile'})
    )


def top_locations(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    if df.empty or 'user_location' not in df.columns:
        return pd.DataFrame()
    return (
        df.dropna(subset=['user_location'])
        .groupby('user_location', as_index=False)
        .agg(count=('review_id', 'count'), avg_rating=('rating', 'mean'))
        .sort_values('count', ascending=False)
        .head(top_n)
    )


def star_band_trend(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    working = df.copy()
    working['submission_time'] = pd.to_datetime(working['submission_time'], errors='coerce')
    working = working.dropna(subset=['submission_time', 'rating'])
    if working.empty:
        return pd.DataFrame()
    working['month_start'] = working['submission_time'].dt.to_period('M').dt.to_timestamp()
    working['low'] = working['rating'].isin([1, 2])
    working['high'] = working['rating'].isin([4, 5])
    grouped = working.groupby('month_start', as_index=False).agg(total=('review_id', 'count'), low_ct=('low', 'sum'), high_ct=('high', 'sum'))
    grouped['pct_low'] = grouped['low_ct'] / grouped['total'].clip(lower=1) * 100
    grouped['pct_high'] = grouped['high_ct'] / grouped['total'].clip(lower=1) * 100
    return grouped.sort_values('month_start')


def select_relevant_reviews(df: pd.DataFrame, question: str, max_reviews: int = 22) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    working = df.copy()
    blob = working['title_and_text'].fillna('').astype(str).str.lower().str.replace(r'\s+', ' ', regex=True)
    tokens = tokenize(question)
    is_problem_query = any(
        token in {'defect', 'broken', 'issue', 'problem', 'bad', 'fail', 'broke'}
        for token in tokens
    )

    # ── Vectorised scoring ──────────────────────────────────────────────────
    # Token relevance: 3 points per token present + count of occurrences
    token_score = pd.Series(0.0, index=working.index)
    for token in tokens:
        present = blob.str.contains(re.escape(token), regex=True)
        occurrences = blob.str.count(re.escape(token))
        token_score += present * 3 + occurrences

    # Problem-query bonus: lower-rated reviews are more relevant for defect Qs
    rating_num = pd.to_numeric(working.get('rating', pd.Series(dtype=float)), errors='coerce')
    problem_bonus = pd.Series(0.0, index=working.index)
    if is_problem_query:
        problem_bonus = (6 - rating_num).clip(lower=0).fillna(0)

    # Organic review bonus
    organic_bonus = (~working.get('incentivized_review', pd.Series(False, index=working.index))
                     .fillna(False).astype(bool)) * 0.5

    # Length bonus: up to 2 points for longer reviews
    word_count = pd.to_numeric(
        working.get('review_length_words', blob.str.split().str.len()),
        errors='coerce',
    ).fillna(0)
    length_bonus = (word_count / 60).clip(upper=2.0)

    working['_score'] = token_score + problem_bonus + organic_bonus + length_bonus
    ranked = working.sort_values(['_score', 'submission_time'], ascending=[False, False], na_position='last')
    combined = pd.concat(
        [
            ranked.head(max_reviews),
            df[df['rating'].between(1, 2, inclusive='both')].head(max_reviews // 3 or 1),
            df[df['rating'].between(4, 5, inclusive='both')].head(max_reviews // 3 or 1),
        ],
        ignore_index=True,
    ).drop_duplicates(subset=['review_id'])
    return combined.head(max_reviews).drop(columns=['_score'], errors='ignore')


def locale_to_region_label(locale: Any) -> str:
    raw = str(locale or '').replace('-', '_').strip()
    if not raw:
        return 'Unknown'
    parts = [part for part in raw.split('_') if part]
    country = (parts[-1] if parts else raw).upper()
    country = ''.join(ch for ch in country if ch.isalpha())
    if not country:
        return 'Unknown'
    return REGION_NAME_MAP.get(country, country)


def parse_smoothing_window(label: str) -> int:
    text = str(label or '').lower()
    if text.startswith('none'):
        return 1
    digits = ''.join(ch for ch in text if ch.isdigit())
    return int(digits) if digits else 1


def cumulative_avg_region_trend(
    df: pd.DataFrame,
    *,
    organic_only: bool = False,
    top_n: int = 2,
    smoothing_label: str = '7-day',
) -> Tuple[pd.DataFrame, List[str]]:
    if df.empty or 'submission_time' not in df.columns or 'rating' not in df.columns:
        return pd.DataFrame(), []

    working = df.copy()
    working['submission_time'] = pd.to_datetime(working['submission_time'], errors='coerce')
    working['rating'] = pd.to_numeric(working['rating'], errors='coerce')
    working = working.dropna(subset=['submission_time', 'rating']).copy()

    if organic_only and 'incentivized_review' in working.columns:
        working = working[~working['incentivized_review'].fillna(False)].copy()

    if working.empty:
        return pd.DataFrame(), []

    working['day'] = working['submission_time'].dt.floor('D')
    working['region'] = working.get('content_locale', pd.Series(index=working.index, dtype='object')).map(locale_to_region_label).fillna('Unknown')

    full_days = pd.date_range(working['day'].min(), working['day'].max(), freq='D')
    base = pd.DataFrame({'day': full_days})

    overall = working.groupby('day', as_index=False).agg(daily_volume=('review_id', 'count'), rating_sum=('rating', 'sum'))
    trend = base.merge(overall, on='day', how='left').fillna({'daily_volume': 0, 'rating_sum': 0})
    trend['daily_volume'] = trend['daily_volume'].astype(int)
    overall_denom = trend['daily_volume'].cumsum()
    trend['overall_cum_avg'] = np.where(overall_denom > 0, trend['rating_sum'].cumsum() / overall_denom, np.nan)

    region_counts = (
        working[working['region'] != 'Unknown']
        .groupby('region')['review_id']
        .count()
        .sort_values(ascending=False)
    )
    regions = region_counts.head(top_n).index.tolist()
    if not regions and 'Unknown' in set(working['region']):
        regions = ['Unknown']

    for region in regions:
        region_df = working[working['region'] == region].groupby('day', as_index=False).agg(region_volume=('review_id', 'count'), rating_sum=('rating', 'sum'))
        region_df = base.merge(region_df, on='day', how='left').fillna({'region_volume': 0, 'rating_sum': 0})
        denom = region_df['region_volume'].cumsum()
        trend[f'{region}_cum_avg'] = np.where(denom > 0, region_df['rating_sum'].cumsum() / denom, np.nan)

    smoothing_window = parse_smoothing_window(smoothing_label)
    if smoothing_window > 1:
        for col in [col for col in trend.columns if col.endswith('_cum_avg')]:
            trend[col] = trend[col].rolling(smoothing_window, min_periods=1).mean()

    return trend.sort_values('day').reset_index(drop=True), regions


def build_volume_bar_series(trend: pd.DataFrame, volume_mode: str):
    if trend is None or trend.empty:
        return pd.DataFrame(columns=['x', 'volume', 'width_ms', 'label']), 'Reviews/day'

    working = trend[['day', 'daily_volume']].copy()
    working['day'] = pd.to_datetime(working['day'], errors='coerce')
    working['daily_volume'] = pd.to_numeric(working['daily_volume'], errors='coerce').fillna(0)
    working = working.dropna(subset=['day'])
    if working.empty:
        return pd.DataFrame(columns=['x', 'volume', 'width_ms', 'label']), 'Reviews/day'

    mode = str(volume_mode or 'Reviews/day')
    if mode == 'Reviews/week':
        working['bucket_start'] = working['day'].dt.to_period('W-SUN').dt.start_time
        grouped = working.groupby('bucket_start', as_index=False).agg(volume=('daily_volume', 'sum'))
        grouped['bucket_days'] = 7.0
        grouped['label'] = grouped['bucket_start'].dt.strftime('Week of %Y-%m-%d')
        axis_title = 'Reviews/week'
    elif mode == 'Reviews/month':
        working['bucket_start'] = working['day'].dt.to_period('M').dt.start_time
        grouped = working.groupby('bucket_start', as_index=False).agg(volume=('daily_volume', 'sum'))
        grouped['bucket_days'] = grouped['bucket_start'].dt.days_in_month.astype(float)
        grouped['label'] = grouped['bucket_start'].dt.strftime('%Y-%m')
        axis_title = 'Reviews/month'
    else:
        grouped = working.rename(columns={'day': 'bucket_start', 'daily_volume': 'volume'}).copy()
        grouped['bucket_days'] = 1.0
        grouped['label'] = grouped['bucket_start'].dt.strftime('%Y-%m-%d')
        axis_title = 'Reviews/day'

    grouped['x'] = grouped['bucket_start'] + pd.to_timedelta(grouped['bucket_days'] / 2.0, unit='D')
    grouped['width_ms'] = grouped['bucket_days'].map(lambda days: int(pd.Timedelta(days=max(float(days) - 0.15, 0.35)).total_seconds() * 1000))
    grouped['volume'] = pd.to_numeric(grouped['volume'], errors='coerce').fillna(0).astype(int)
    return grouped[['x', 'volume', 'width_ms', 'label']], axis_title


SYMPTOM_NON_VALUES = set(NON_VALUES) | {'NOT MENTIONED'}


def _dedupe_symptom_cols(values: Sequence[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def get_symptom_col_lists(df: pd.DataFrame):
    cleaned = [str(col).strip() for col in df.columns]
    man_det = [col for col in cleaned if col.lower() in {f'symptom {idx}' for idx in range(1, 11)}]
    man_del = [col for col in cleaned if col.lower() in {f'symptom {idx}' for idx in range(11, 21)}]
    ai_det = [col for col in cleaned if col.lower().startswith('ai symptom detractor')]
    ai_del = [col for col in cleaned if col.lower().startswith('ai symptom delighter')]
    return _dedupe_symptom_cols(man_det + ai_det), _dedupe_symptom_cols(man_del + ai_del)


def detect_symptom_state(df: pd.DataFrame) -> str:
    det_cols, del_cols = get_symptom_col_lists(df)

    def has_values(cols: Sequence[str]) -> bool:
        for col in cols:
            if col not in df.columns:
                continue
            series = df[col].astype('string').fillna('').str.strip()
            valid = (series != '') & (~series.str.upper().isin(SYMPTOM_NON_VALUES)) & (~series.str.startswith('<'))
            if valid.any():
                return True
        return False

    has_det = has_values(det_cols)
    has_del = has_values(del_cols)
    if has_det and has_del:
        return 'full'
    if has_det or has_del:
        return 'partial'
    return 'none'


def _empty_symptom_table() -> pd.DataFrame:
    return pd.DataFrame(columns=['Item', 'Avg Star', 'Mentions', '% Tagged Reviews', 'Avg Tags/Review'])



def _prepare_symptom_long(df_in: pd.DataFrame, symptom_cols: Sequence[str]) -> tuple[pd.DataFrame, int]:
    if df_in is None or df_in.empty:
        return pd.DataFrame(columns=['__row', 'symptom', 'symptom_count', 'review_weight', 'star']), 0
    targets = {str(col).strip() for col in symptom_cols if str(col).strip()}
    if not targets:
        return pd.DataFrame(columns=['__row', 'symptom', 'symptom_count', 'review_weight', 'star']), 0
    col_names = [str(col).strip() for col in df_in.columns]
    positions = [idx for idx, name in enumerate(col_names) if name in targets]
    if not positions:
        return pd.DataFrame(columns=['__row', 'symptom', 'symptom_count', 'review_weight', 'star']), 0

    block = df_in.iloc[:, positions].copy()
    block.columns = [f'__sym_{idx}' for idx in range(block.shape[1])]
    block.insert(0, '__row', np.arange(len(block), dtype=int))
    long = block.melt(id_vars='__row', value_name='symptom', var_name='__col')
    text = long['symptom'].astype('string').fillna('').str.strip()
    mask = (text != '') & (~text.str.upper().isin(SYMPTOM_NON_VALUES)) & (~text.str.startswith('<'))
    long = long.loc[mask, ['__row']].copy()
    if long.empty:
        return pd.DataFrame(columns=['__row', 'symptom', 'symptom_count', 'review_weight', 'star']), 0

    long['symptom'] = text.loc[mask].str.title()
    long = long.drop_duplicates(subset=['__row', 'symptom'])
    if long.empty:
        return pd.DataFrame(columns=['__row', 'symptom', 'symptom_count', 'review_weight', 'star']), 0

    counts = long.groupby('__row', dropna=False)['symptom'].transform('nunique').astype(float)
    long['symptom_count'] = counts
    long['review_weight'] = (1.0 / counts.replace(0, np.nan)).fillna(0.0)
    if 'rating' in df_in.columns:
        stars = pd.to_numeric(df_in.reset_index(drop=True)['rating'], errors='coerce').rename('star')
        long = long.join(stars, on='__row')
    else:
        long['star'] = np.nan
    return long, int(long['__row'].nunique())



def analyze_symptoms_fast(df_in: pd.DataFrame, symptom_cols: Sequence[str]) -> pd.DataFrame:
    long, symptomized_reviews = _prepare_symptom_long(df_in, symptom_cols)
    if long.empty:
        return _empty_symptom_table()

    grouped = long.groupby('symptom', dropna=False)
    mention_reviews = grouped['__row'].nunique().astype(int)
    avg_tags = grouped['symptom_count'].mean()
    avg_stars = grouped['star'].mean() if 'star' in long.columns else pd.Series(index=mention_reviews.index, dtype=float)
    weighted_mentions = grouped['review_weight'].sum()

    out = pd.DataFrame(
        {
            'Item': [str(item).title() for item in mention_reviews.index.tolist()],
            'Mentions': mention_reviews.values.astype(int),
            '% Tagged Reviews': (mention_reviews.values / max(symptomized_reviews, 1) * 100).round(1).astype(str) + '%',
            'Avg Star': [round(float(avg_stars[item]), 1) if item in avg_stars and not pd.isna(avg_stars[item]) else None for item in mention_reviews.index],
            'Avg Tags/Review': np.round(avg_tags.values.astype(float), 2),
            '__Weighted Mentions': weighted_mentions.values.astype(float),
            '__Mention Reviews': mention_reviews.values.astype(int),
            '__Symptomized Reviews': symptomized_reviews,
            '__All Reviews': int(len(df_in)),
        }
    ).sort_values(['Mentions', '__Weighted Mentions', 'Item'], ascending=[False, False, True], ignore_index=True)
    out.attrs['symptomized_review_count'] = symptomized_reviews
    out.attrs['all_review_count'] = int(len(df_in))
    return out


def _infer_symptom_total_reviews(tbl: pd.DataFrame) -> int:
    if tbl is None or tbl.empty:
        return 0
    if '__All Reviews' in tbl.columns:
        total = int(pd.to_numeric(tbl['__All Reviews'], errors='coerce').fillna(0).max() or 0)
        if total > 0:
            return total
    pct_col = '% Tagged Reviews' if '% Tagged Reviews' in tbl.columns else ('% Total' if '% Total' in tbl.columns else None)
    if pct_col is None:
        total = int(pd.to_numeric(tbl.get('Mentions'), errors='coerce').fillna(0).max() or 0)
        return max(total, 0)
    pct = pd.to_numeric(tbl[pct_col].astype(str).str.replace('%', '', regex=False), errors='coerce')
    mentions = pd.to_numeric(tbl.get('Mentions'), errors='coerce').fillna(0)
    ratios = mentions / (pct / 100.0)
    ratios = ratios[(pct > 0) & ratios.notna() & (ratios > 0)]
    if ratios.empty:
        total = int(mentions.max() or 0)
        return max(total, 0)
    return max(int(round(float(ratios.median()))), 1)



def _compute_detailed_symptom_impact(
    df_in: pd.DataFrame,
    symptom_cols: Sequence[str],
    baseline: float,
    *,
    kind: str,
) -> tuple[pd.DataFrame, int]:
    long, symptomized_reviews = _prepare_symptom_long(df_in, symptom_cols)
    if long.empty:
        return pd.DataFrame(columns=['Mention Reviews', 'Avg Tags/Review', 'Avg Star', 'Weighted Mentions', 'Net Hit Raw']), 0

    stars = pd.to_numeric(long['star'], errors='coerce')
    if str(kind).lower().startswith('del'):
        gap = (stars - float(baseline)).clip(lower=0)
    else:
        gap = (float(baseline) - stars).clip(lower=0)
    long['gap'] = gap.fillna(0.0)
    long['attributed_gap'] = long['review_weight'].astype(float) * long['gap'].astype(float)

    grouped = long.groupby('symptom', dropna=False)
    out = grouped.agg(
        **{
            'Mention Reviews': ('__row', 'nunique'),
            'Avg Tags/Review': ('symptom_count', 'mean'),
            'Avg Star': ('star', 'mean'),
            'Weighted Mentions': ('review_weight', 'sum'),
            'Net Hit Raw': ('attributed_gap', 'sum'),
        }
    )
    return out, symptomized_reviews



def add_net_hit(
    tbl: pd.DataFrame,
    avg_rating: Any,
    total_reviews: int | None = None,
    *,
    kind: str = 'detractors',
    shrink_k: float = 3.0,
    detail_df: pd.DataFrame | None = None,
    symptom_cols: Sequence[str] | None = None,
) -> pd.DataFrame:
    return symptom_add_net_hit(
        tbl,
        avg_rating,
        total_reviews=total_reviews,
        kind=kind,
        shrink_k=shrink_k,
        detail_df=detail_df,
        symptom_cols=symptom_cols,
    )

