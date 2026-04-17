from __future__ import annotations

import hashlib
import io
import json
import os
import re
from typing import Any, Dict, List, Tuple

import pandas as pd

from .models import ReviewBatchSummary
from .utils import NON_VALUES, dedupe_keep_order, safe_bool, safe_int, safe_text


def powerreviews_bool_from_bottom_line(value: Any):
    text = safe_text(value).lower()
    if text in {"yes", "recommended", "true"}:
        return True
    if text in {"no", "false", "not recommended"}:
        return False
    return pd.NA


def extract_age_group(value: Any):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    payload = value
    if isinstance(payload, str):
        stripped = payload.strip()
        if not stripped:
            return None
        try:
            payload = json.loads(stripped)
        except Exception:
            return None
    if not isinstance(payload, dict):
        return None
    for key, raw in payload.items():
        if "age" not in str(key).lower():
            continue
        candidate = raw.get("Value") or raw.get("Label") if isinstance(raw, dict) else raw
        candidate = safe_text(candidate)
        if candidate and candidate.lower() not in {"nan", "none", "null", "unknown", "prefer not to say"}:
            return candidate
    return None


def _ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    return df

def _all_dataframe_columns(df: pd.DataFrame) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for col in df.columns:
        label = str(col)
        if label in seen:
            continue
        seen.add(label)
        ordered.append(label)
    return ordered


def _select_column_series(df: pd.DataFrame, column: str) -> pd.Series | None:
    if not column or column not in df.columns:
        return None
    selected = df.loc[:, column]
    if isinstance(selected, pd.Series):
        return selected.copy()
    if isinstance(selected, pd.DataFrame):
        if selected.shape[1] == 1:
            series = selected.iloc[:, 0].copy()
            series.name = column
            return series
        cleaned = selected.copy()
        for idx in range(cleaned.shape[1]):
            ser = cleaned.iloc[:, idx]
            if pd.api.types.is_object_dtype(ser) or pd.api.types.is_string_dtype(ser):
                cleaned.iloc[:, idx] = ser.astype("string").str.strip().replace("", pd.NA)
        collapsed = cleaned.bfill(axis=1).iloc[:, 0]
        collapsed.name = column
        return collapsed
    return pd.Series(selected, index=df.index, name=column)


def _collapse_duplicate_named_columns(df: pd.DataFrame) -> pd.DataFrame:
    labels = [str(col) for col in df.columns]
    if len(labels) == len(set(labels)):
        return df
    ordered: List[str] = []
    data: Dict[str, pd.Series] = {}
    for label in labels:
        if label in data:
            continue
        series = _select_column_series(df, label)
        if series is None:
            continue
        ordered.append(label)
        data[label] = series
    return pd.DataFrame({label: data[label] for label in ordered}, index=df.index)


def _is_incentivized_bv(review: Dict[str, Any]) -> bool:
    badges = [str(badge).lower() for badge in (review.get("BadgesOrder") or [])]
    if any("incentivized" in badge for badge in badges):
        return True
    context = review.get("ContextDataValues") or {}
    if isinstance(context, dict):
        for key, value in context.items():
            if "incentivized" in str(key).lower():
                flag = str((value.get("Value", "") if isinstance(value, dict) else value)).strip().lower()
                if flag in {"", "true", "1", "yes"}:
                    return True
    return False


def flatten_bv_review(review: Dict[str, Any]) -> Dict[str, Any]:
    photos = review.get("Photos") or []
    urls: List[str] = []
    for photo in photos:
        sizes = photo.get("Sizes") or {}
        for size_name in ["large", "normal", "thumbnail"]:
            url = (sizes.get(size_name) or {}).get("Url")
            if url:
                urls.append(url)
                break
    syndication_source = review.get("SyndicationSource") or {}
    return {
        "review_id": review.get("Id"),
        "product_id": review.get("ProductId"),
        "original_product_name": review.get("OriginalProductName"),
        "title": safe_text(review.get("Title")),
        "review_text": safe_text(review.get("ReviewText")),
        "rating": review.get("Rating"),
        "is_recommended": review.get("IsRecommended"),
        "user_nickname": review.get("UserNickname"),
        "author_id": review.get("AuthorId"),
        "user_location": review.get("UserLocation"),
        "content_locale": review.get("ContentLocale"),
        "submission_time": review.get("SubmissionTime"),
        "moderation_status": review.get("ModerationStatus"),
        "campaign_id": review.get("CampaignId"),
        "source_client": review.get("SourceClient"),
        "is_featured": review.get("IsFeatured"),
        "is_syndicated": review.get("IsSyndicated"),
        "syndication_source_name": syndication_source.get("Name"),
        "is_ratings_only": review.get("IsRatingsOnly"),
        "total_positive_feedback_count": review.get("TotalPositiveFeedbackCount"),
        "badges": ", ".join(str(item) for item in (review.get("BadgesOrder") or [])),
        "context_data_json": json.dumps(review.get("ContextDataValues") or {}, ensure_ascii=False),
        "photos_count": len(photos),
        "photo_urls": " | ".join(urls),
        "incentivized_review": _is_incentivized_bv(review),
        "raw_json": json.dumps(review, ensure_ascii=False),
    }


def powerreviews_media_urls(media_items: List[Dict[str, Any]]) -> List[str]:
    urls: List[str] = []
    for item in media_items or []:
        if not isinstance(item, dict):
            continue
        for key in ["large_url", "normal_url", "thumbnail_url", "url", "fullsize_url", "media_url", "src", "link"]:
            value = item.get(key)
            if value:
                urls.append(str(value))
                break
    return urls


def powerreviews_submission_iso(value: Any):
    ts = pd.to_datetime(value, unit="ms", utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    try:
        return ts.isoformat()
    except Exception:
        return str(ts)


def flatten_powerreviews_review(
    review: Dict[str, Any],
    *,
    page_id: str = "",
    product_name: str = "",
    retailer: str = "",
    product_url: str = "",
) -> Dict[str, Any]:
    details = review.get("details") or {}
    metrics = review.get("metrics") or {}
    media = review.get("media") or []
    photo_urls = powerreviews_media_urls(media)
    product_id = safe_text(details.get("product_page_id") or review.get("page_id") or page_id)

    incentivized = False
    badges = review.get("badges") or {}
    for key, value in badges.items():
        lower_key = str(key).lower()
        if any(token in lower_key for token in ["sampling", "sample", "sweepstakes", "incentivized", "influencer"]):
            if bool(value):
                incentivized = True
                break

    return {
        "review_id": review.get("review_id") or review.get("ugc_id") or review.get("internal_review_id") or review.get("legacy_id"),
        "product_id": product_id,
        "base_sku": product_id,
        "sku_item": product_id,
        "original_product_name": safe_text(product_name),
        "title": safe_text(details.get("headline") or review.get("headline")),
        "review_text": safe_text(details.get("comments") or review.get("comments")),
        "rating": metrics.get("rating") or details.get("rating"),
        "is_recommended": powerreviews_bool_from_bottom_line(details.get("bottom_line") or review.get("bottom_line")),
        "user_nickname": safe_text(details.get("nickname") or review.get("nickname")),
        "author_id": review.get("author_id"),
        "user_location": safe_text(details.get("location") or review.get("location")),
        "content_locale": safe_text(details.get("locale") or review.get("locale")),
        "submission_time": powerreviews_submission_iso(details.get("created_date") or review.get("created_date")),
        "moderation_status": review.get("status") or pd.NA,
        "campaign_id": pd.NA,
        "source_client": pd.NA,
        "is_featured": pd.NA,
        "is_syndicated": bool(review.get("is_syndicated") or badges.get("is_syndicated") or False),
        "syndication_source_name": pd.NA,
        "is_ratings_only": False,
        "total_positive_feedback_count": metrics.get("helpful_votes"),
        "badges": ", ".join(str(key) for key, value in (badges or {}).items() if bool(value)),
        "context_data_json": json.dumps(details.get("properties") or [], ensure_ascii=False),
        "photos_count": len(photo_urls),
        "photo_urls": " | ".join(photo_urls),
        "incentivized_review": incentivized,
        "raw_json": json.dumps(review, ensure_ascii=False),
        "retailer": retailer,
        "post_link": product_url,
        "source_system": "PowerReviews",
    }


def okendo_media_urls(review: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    media_candidates = review.get("media") or review.get("reviewMedia") or review.get("mediaItems") or []
    for item in media_candidates:
        if not isinstance(item, dict):
            continue
        for key in ["url", "src", "fullsizeUrl", "fullsize_url", "largeUrl", "large_url", "thumbnailUrl", "thumbnail_url", "mediaUrl", "media_url"]:
            value = item.get(key)
            if value:
                urls.append(str(value))
                break
    return dedupe_keep_order(urls)


def _okendo_reviewer_attributes_map(reviewer: Dict[str, Any]) -> Dict[str, Any]:
    attrs = reviewer.get("attributes") or []
    out: Dict[str, Any] = {}
    for item in attrs:
        if not isinstance(item, dict):
            continue
        title = safe_text(item.get("title"))
        if not title:
            continue
        out[title] = {"Value": item.get("value")}
    return out


def _okendo_age_group(reviewer: Dict[str, Any]) -> Any:
    for item in reviewer.get("attributes") or []:
        if not isinstance(item, dict):
            continue
        title = safe_text(item.get("title")).lower()
        if "age" in title:
            value = item.get("value")
            if isinstance(value, list):
                return ", ".join(str(v) for v in value if safe_text(v))
            return safe_text(value) or pd.NA
    return pd.NA


def _okendo_user_location(reviewer: Dict[str, Any]) -> str:
    location = reviewer.get("location") or {}
    country = (location.get("country") or {}) if isinstance(location, dict) else {}
    country_name = safe_text(country.get("name"))
    if country_name:
        return country_name
    for item in reviewer.get("attributes") or []:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "location":
            value = item.get("value") or {}
            if isinstance(value, dict):
                return safe_text(value.get("countryName") or value.get("countryCode"))
            return safe_text(value)
    return ""


def flatten_okendo_review(
    review: Dict[str, Any],
    *,
    product_id: str = "",
    product_name: str = "",
    retailer: str = "",
    product_url: str = "",
) -> Dict[str, Any]:
    reviewer = review.get("reviewer") or {}
    photo_urls = okendo_media_urls(review)
    okendo_product_id = safe_text(review.get("productId") or product_id)
    variant_id = safe_text(review.get("variantId"))
    review_product_url = safe_text(review.get("productUrl") or product_url)
    if review_product_url.startswith("//"):
        review_product_url = "https:" + review_product_url
    context_attrs = _okendo_reviewer_attributes_map(reviewer)

    return {
        "review_id": review.get("reviewId") or review.get("id"),
        "product_id": okendo_product_id,
        "base_sku": okendo_product_id,
        "sku_item": variant_id or okendo_product_id,
        "original_product_name": safe_text(review.get("productName") or product_name),
        "title": safe_text(review.get("title")),
        "review_text": safe_text(review.get("body")),
        "rating": review.get("rating"),
        "is_recommended": review.get("isRecommended"),
        "user_nickname": safe_text(reviewer.get("displayName")),
        "author_id": review.get("subscriberId"),
        "user_location": _okendo_user_location(reviewer),
        "content_locale": safe_text(review.get("languageCode")),
        "submission_time": review.get("dateCreated") or review.get("dateSubmitted"),
        "moderation_status": review.get("status") or pd.NA,
        "campaign_id": pd.NA,
        "source_client": "Okendo",
        "is_featured": review.get("isFeatured") if "isFeatured" in review else pd.NA,
        "is_syndicated": bool(review.get("syndicatedFromSubscriberId") or False),
        "syndication_source_name": review.get("syndicatedFromSubscriberId") or pd.NA,
        "is_ratings_only": False,
        "total_positive_feedback_count": review.get("helpfulCount"),
        "badges": ", ".join(
            [
                label
                for label, flag in {
                    "verified": reviewer.get("isVerified"),
                    "incentivized": review.get("isIncentivized"),
                    "syndicated": bool(review.get("syndicatedFromSubscriberId")),
                }.items()
                if bool(flag)
            ]
        ),
        "context_data_json": json.dumps(context_attrs, ensure_ascii=False),
        "photos_count": len(photo_urls),
        "photo_urls": " | ".join(photo_urls),
        "incentivized_review": bool(review.get("isIncentivized") or False),
        "raw_json": json.dumps(review, ensure_ascii=False),
        "retailer": retailer,
        "post_link": review_product_url or product_url,
        "source_system": "Okendo",
        "age_group": _okendo_age_group(reviewer),
    }


def apply_source_metadata(df: pd.DataFrame, *, retailer: str = "", source_system: str = "", post_link: str = "") -> pd.DataFrame:
    out = df.copy()
    if retailer:
        if "retailer" not in out.columns:
            out["retailer"] = retailer
        else:
            out["retailer"] = out["retailer"].fillna("").astype(str)
            out.loc[out["retailer"].str.strip().eq(""), "retailer"] = retailer
    if source_system:
        out["source_system"] = source_system
    if post_link:
        if "post_link" not in out.columns:
            out["post_link"] = post_link
        else:
            out["post_link"] = out["post_link"].fillna("").astype(str)
            out.loc[out["post_link"].str.strip().eq(""), "post_link"] = post_link
    return out


def finalize_df(df: pd.DataFrame) -> pd.DataFrame:
    required = [
        "review_id", "product_id", "base_sku", "sku_item", "product_or_sku",
        "original_product_name", "title", "review_text", "rating", "is_recommended",
        "content_locale", "submission_time", "submission_date", "submission_month",
        "incentivized_review", "is_syndicated", "photos_count", "photo_urls",
        "title_and_text", "retailer", "post_link", "age_group", "user_nickname",
        "user_location", "total_positive_feedback_count", "source_system", "source_file",
        "moderation_status", "moderation_bucket", "campaign_id", "review_origin_group",
        "review_acquisition_channel", "country",
    ]
    df = _ensure_cols(df.copy(), required)
    if df.empty:
        for col in ["has_photos", "has_media", "review_length_chars", "review_length_words", "rating_label", "year_month_sort"]:
            if col not in df.columns:
                df[col] = pd.Series(dtype="object")
        return df

    df["review_id"] = df["review_id"].fillna("").astype(str).str.strip()
    missing = df["review_id"].eq("") | df["review_id"].str.lower().isin({"nan", "none", "null"})
    if missing.any():
        source_series = df.get("source_file", pd.Series(pd.NA, index=df.index)).fillna("").astype(str)
        product_series = df.get("product_id", pd.Series(pd.NA, index=df.index)).fillna("").astype(str)
        base_series = df.get("base_sku", pd.Series(pd.NA, index=df.index)).fillna("").astype(str)
        submitted_series = df.get("submission_time", pd.Series(pd.NA, index=df.index)).fillna("").astype(str)
        title_series = df.get("title", pd.Series(pd.NA, index=df.index)).fillna("").astype(str)
        text_series = df.get("review_text", pd.Series(pd.NA, index=df.index)).fillna("").astype(str)
        generated_ids = []
        for row_position, row_index in enumerate(df.index[missing]):
            payload = "|".join(
                [
                    source_series.loc[row_index],
                    product_series.loc[row_index],
                    base_series.loc[row_index],
                    submitted_series.loc[row_index],
                    title_series.loc[row_index],
                    text_series.loc[row_index],
                    str(row_position),
                ]
            )
            digest = hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()[:16]
            generated_ids.append(f"review_{digest}")
        df.loc[missing, "review_id"] = generated_ids

    if "context_data_json" in df.columns:
        df["age_group"] = df["age_group"].fillna(df["context_data_json"].map(extract_age_group))

    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["incentivized_review"] = df["incentivized_review"].astype("boolean").fillna(False).astype(bool)
    df["is_syndicated"] = df["is_syndicated"].astype("boolean").fillna(False).astype(bool)
    df["photos_count"] = pd.to_numeric(df["photos_count"], errors="coerce").fillna(0).astype(int)
    df["title"] = df["title"].fillna("").astype(str)
    df["review_text"] = df["review_text"].fillna("").astype(str)
    df["submission_time"] = pd.to_datetime(df["submission_time"], errors="coerce", utc=True).dt.tz_convert(None)
    df["submission_date"] = df["submission_time"].dt.date
    df["submission_month"] = df["submission_time"].dt.to_period("M").astype(str)
    df["content_locale"] = df["content_locale"].fillna("").astype(str).replace({"": pd.NA})
    df["base_sku"] = df.get("base_sku", pd.Series(dtype="str")).fillna("").astype(str).str.strip()
    df["sku_item"] = df.get("sku_item", pd.Series(dtype="str")).fillna("").astype(str).str.strip()
    df["product_id"] = df["product_id"].fillna("").astype(str).str.strip()
    fallback = df["product_id"].where(df["product_id"].ne(""), df["base_sku"])
    df["product_or_sku"] = df["sku_item"].where(df["sku_item"].ne(""), fallback)
    df["product_or_sku"] = df["product_or_sku"].fillna("").astype(str).str.strip().replace({"": pd.NA})
    df["title_and_text"] = (df["title"].str.strip() + " " + df["review_text"].str.strip()).str.strip()
    df["has_photos"] = df["photos_count"] > 0
    df["has_media"] = df["has_photos"]
    df["review_length_chars"] = df["review_text"].str.len()
    df["review_length_words"] = df["review_text"].str.split().str.len().fillna(0).astype(int)
    df["rating_label"] = df["rating"].map(lambda x: f"{int(x)} star" if pd.notna(x) else "Unknown")
    df["year_month_sort"] = pd.to_datetime(df["submission_month"], format="%Y-%m", errors="coerce")

    df["country"] = df.get("country", pd.Series(pd.NA, index=df.index)).astype("string").fillna("").str.strip().replace({"": pd.NA})
    if df["country"].isna().all() and "content_locale" in df.columns:
        df["country"] = df["content_locale"].astype("string").str.split("_").str[-1].replace({"": pd.NA})

    df["moderation_status"] = df.get("moderation_status", pd.Series(pd.NA, index=df.index)).map(_normalize_moderation_status).replace({"": pd.NA})
    df["moderation_bucket"] = df.get("moderation_bucket", pd.Series(pd.NA, index=df.index)).astype("string").fillna("").str.strip()
    bucket_fallback = df["moderation_status"].map(_moderation_bucket)
    df["moderation_bucket"] = df["moderation_bucket"].where(df["moderation_bucket"].ne(""), bucket_fallback).replace({"": pd.NA})

    df["campaign_id"] = df.get("campaign_id", pd.Series(pd.NA, index=df.index)).astype("string").fillna("").str.strip().replace({"": pd.NA})
    df["review_origin_group"] = df.get("review_origin_group", pd.Series(pd.NA, index=df.index)).astype("string").fillna("").str.strip()
    df["review_acquisition_channel"] = df.get("review_acquisition_channel", pd.Series(pd.NA, index=df.index)).astype("string").fillna("").str.strip()
    origin_fallback = [
        _review_origin_group_from_values(incent, syndicated, campaign)
        for incent, syndicated, campaign in zip(df["incentivized_review"], df["is_syndicated"], df["campaign_id"])
    ]
    channel_fallback = [
        _review_acquisition_channel_from_values(incent, campaign)
        for incent, campaign in zip(df["incentivized_review"], df["campaign_id"])
    ]
    df["review_origin_group"] = df["review_origin_group"].where(df["review_origin_group"].ne(""), pd.Series(origin_fallback, index=df.index)).replace({"": pd.NA})
    df["review_acquisition_channel"] = df["review_acquisition_channel"].where(df["review_acquisition_channel"].ne(""), pd.Series(channel_fallback, index=df.index)).replace({"": pd.NA})

    sort_cols = [col for col in ["submission_time", "review_id"] if col in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[False, False], na_position="last").reset_index(drop=True)
    return df


def _pick_col(df: pd.DataFrame, aliases: List[str]):
    lookup = {str(col).strip().lower(): col for col in df.columns}
    for alias in aliases:
        col = lookup.get(str(alias).strip().lower())
        if col:
            return col
    return None


REVIEW_ID_ALIASES = ["Event Id", "Event ID", "Review ID", "Review Id", "Verbatim Id", "Verbatim ID", "Id", "review_id"]
REVIEW_TEXT_ALIASES = ["Review Text", "Review", "Verbatim", "Body", "Content", "review_text"]
TITLE_ALIASES = ["Title", "Review Title", "Review title", "Headline", "title"]
RATING_ALIASES = ["Overall Rating", "Rating (num)", "Rating", "Stars", "Star Rating", "rating"]
DATE_ALIASES = ["Review Submission Date", "Opened date", "Opened Date", "Submission Time", "Review Date", "Date", "submission_time"]
LOCAL_META_ALIASES = {
    "AI Safety": ["AI Safety", "Safety"],
    "AI Reliability": ["AI Reliability", "Reliability"],
    "AI # of Sessions": ["AI # of Sessions", "# of Sessions", "Number of Sessions", "Sessions"],
}
SYMPTOM_NON_VALUES = set(NON_VALUES) | {"NOT MENTIONED"}


def _series_alias(df: pd.DataFrame, aliases: List[str]) -> pd.Series:
    col = _pick_col(df, aliases)
    if col is None:
        return pd.Series([pd.NA] * len(df), index=df.index)
    return df[col]


def _parse_flag(value: Any, *, pos: List[str], neg: List[str]):
    text = safe_text(value).strip().lower()
    if text in {"", "nan", "none", "null", "n/a", "na", "unknown", "unspecified"}:
        return pd.NA
    normalized = re.sub(r"[^a-z0-9]+", " ", text).strip()
    pos_terms = [re.sub(r"[^a-z0-9]+", " ", safe_text(candidate).strip().lower()).strip() for candidate in pos if safe_text(candidate).strip()]
    neg_terms = [re.sub(r"[^a-z0-9]+", " ", safe_text(candidate).strip().lower()).strip() for candidate in neg if safe_text(candidate).strip()]
    if normalized in neg_terms:
        return False
    if normalized in pos_terms:
        return True
    if normalized.startswith(("not ", "non ")):
        return False
    if re.fullmatch(r"[01](?: 0+)?", normalized):
        return normalized.startswith("1")
    if any(term and term in normalized for term in neg_terms):
        return False
    if any(term and term in normalized for term in pos_terms):
        return True
    return pd.NA


def _normalize_moderation_status(value: Any) -> str:
    text = safe_text(value).strip().upper()
    if not text or text in {"NAN", "NONE", "NULL", "N/A"}:
        return ""
    if text in {"APPROVED", "PUBLISHED", "LIVE"}:
        return "APPROVED"
    if text in {"SUBMITTED", "PENDING", "INREVIEW", "IN_REVIEW", "UNDERREVIEW", "UNDER_REVIEW"}:
        return "SUBMITTED"
    if text in {"REJECTED", "DECLINED", "DENIED"}:
        return "REJECTED"
    if text in {"REMOVEDBYCLIENT", "REMOVED", "DELETED", "SUPPRESSED"}:
        return "REMOVEDBYCLIENT"
    return text


def _moderation_bucket(value: Any) -> str:
    status = _normalize_moderation_status(value)
    if status == "APPROVED":
        return "Approved"
    if status == "SUBMITTED":
        return "Pending"
    if status == "REJECTED":
        return "Rejected"
    if status == "REMOVEDBYCLIENT":
        return "Removed"
    return "Unknown"


def _campaign_lower(value: Any) -> str:
    return safe_text(value).strip().lower()


def _review_origin_group_from_values(incentivized: Any, syndicated: Any, campaign_id: Any) -> str:
    campaign = _campaign_lower(campaign_id)
    is_incentivized = safe_bool(incentivized, False)
    is_syndicated = safe_bool(syndicated, False)
    if is_syndicated:
        return "Syndicated"
    if is_incentivized or any(token in campaign for token in ["bvsampling", "tryit", "sampling", "voxbox", "mavrck", "influenster_voxbox"]):
        return "Seeded / Incentivized"
    return "Organic"


def _review_acquisition_channel_from_values(incentivized: Any, campaign_id: Any) -> str:
    campaign = _campaign_lower(campaign_id)
    if not campaign:
        return "Organic / Standard"
    if "influenster_voxbox" in campaign or "voxbox" in campaign:
        return "Influenster VoxBox"
    if "influenster" in campaign:
        return "Influenster"
    if "mavrck" in campaign:
        return "Mavrck / Creator"
    if any(token in campaign for token in ["bvsampling", "tryit", "sampling", "bvt"]):
        return "Sampling / TryIt"
    if "followup_mpr" in campaign or "followup" in campaign:
        return "Follow-up prompt"
    if "mobile_review_display" in campaign:
        return "Mobile review display"
    if "review_display" in campaign:
        return "Review display"
    if "mobile_rating_summary" in campaign:
        return "Mobile rating summary"
    if "rating_summary" in campaign:
        return "Rating summary"
    if "reviewsource_import" in campaign or "reviewsource_api" in campaign:
        return "Imported review source"
    if "pie_mpr" in campaign or campaign.startswith("bv_pie"):
        return "Post-interaction email"
    if "social_alert" in campaign:
        return "Social alert"
    if campaign.startswith("whol"):
        return "Wholesale / Partner"
    if safe_bool(incentivized, False):
        return "Seeded / Incentivized"
    return "Organic / Standard"


def _local_symptom_columns(columns: List[Any]) -> List[str]:
    out: List[str] = []
    for col in columns:
        name = str(col).strip()
        lower = name.lower()
        if lower.startswith("ai symptom detractor") or lower.startswith("ai symptom delighter"):
            out.append(name)
            continue
        if re.fullmatch(r"symptom\s+(?:[1-9]|10|1[1-9]|20)", lower):
            out.append(name)
    return out


def _normalize_symptom_series(series: pd.Series) -> pd.Series:
    text = series.astype("string").fillna("").str.strip()
    valid = (text != "") & (~text.str.upper().isin(SYMPTOM_NON_VALUES)) & (~text.str.startswith("<"))
    return text.where(valid, pd.NA).str.title()


def _score_uploaded_sheet(columns: List[Any]) -> int:
    lowered = {str(col).strip().lower() for col in columns}
    score = 0
    if any(alias.lower() in lowered for alias in REVIEW_TEXT_ALIASES):
        score += 4
    if any(alias.lower() in lowered for alias in RATING_ALIASES):
        score += 3
    if any(alias.lower() in lowered for alias in REVIEW_ID_ALIASES):
        score += 3
    if any(alias.lower() in lowered for alias in TITLE_ALIASES):
        score += 1
    if any(alias.lower() in lowered for alias in DATE_ALIASES):
        score += 1
    if _local_symptom_columns(list(columns)):
        score += 2
    return score


def _read_best_uploaded_excel_sheet(raw: bytes) -> Tuple[pd.DataFrame, str]:
    bio = io.BytesIO(raw)
    xls = pd.ExcelFile(bio)
    if not xls.sheet_names:
        return pd.DataFrame(), ""
    best_name = xls.sheet_names[0]
    best_score = -1
    for sheet_name in xls.sheet_names:
        try:
            headers = list(pd.read_excel(xls, sheet_name=sheet_name, nrows=0).columns)
        except Exception:
            headers = []
        score = _score_uploaded_sheet(headers)
        if score > best_score:
            best_score = score
            best_name = sheet_name
    return pd.read_excel(xls, sheet_name=best_name), best_name


def normalize_uploaded_df(raw_df: pd.DataFrame, *, source_name: str = "", include_local_symptomization: bool = False) -> pd.DataFrame:
    working = raw_df.copy()
    working.columns = [str(col).strip() for col in working.columns]
    working = _collapse_duplicate_named_columns(working)
    normalized = pd.DataFrame(index=working.index)
    normalized["review_id"] = _series_alias(working, REVIEW_ID_ALIASES)
    normalized["product_id"] = _series_alias(working, ["Product ID", "Product Id", "ProductId", "Model (SKU)", "Model SKU", "Base SKU", "BaseSKU"])
    normalized["base_sku"] = _series_alias(working, ["Base SKU", "Model (SKU)", "Model SKU", "BaseSKU"])
    normalized["sku_item"] = _series_alias(working, ["SKU Item", "Model (SKU)", "Model SKU", "SKU", "Child SKU", "Variant SKU", "Item Number", "Item No"])
    normalized["original_product_name"] = _series_alias(working, ["Product Name", "Product", "Name"])
    normalized["review_text"] = _series_alias(working, REVIEW_TEXT_ALIASES)
    normalized["title"] = _series_alias(working, TITLE_ALIASES)
    normalized["post_link"] = _series_alias(working, ["Post Link", "Web Link", "URL", "Review URL", "Product URL", "Product Page URL"])
    normalized["rating"] = _series_alias(working, RATING_ALIASES)
    normalized["submission_time"] = _series_alias(working, DATE_ALIASES)
    normalized["content_locale"] = _series_alias(working, ["Review Display Locale", "Content Locale", "Locale", "Reviewer Location", "Location", "Country"])
    normalized["retailer"] = _series_alias(working, ["Retailer", "Merchant", "Channel", "Source"])
    normalized["age_group"] = _series_alias(working, ["Age Group", "Age", "Age Range", "Age (CDV)"])
    normalized["user_location"] = _series_alias(working, ["Reviewer Location", "Location", "Country"])
    normalized["country"] = _series_alias(working, ["Country", "Country Code"])
    normalized["user_nickname"] = _series_alias(working, ["Reviewer Display Name", "Reviewer Name", "Display Name", "Nickname"])
    normalized["total_positive_feedback_count"] = _series_alias(working, ["# Helpful Votes", "Helpful Votes", "Total Positive Feedback Count"])
    normalized["is_recommended"] = _series_alias(working, ["Recommend to a Friend (Y/N)", "Is Recommended", "Recommend", "Recommended"])
    normalized["photos_count"] = _series_alias(working, ["Photos", "Photo Count", "Photos Count"])
    normalized["photo_urls"] = pd.NA
    normalized["source_file"] = source_name or pd.NA
    normalized["source_system"] = "Uploaded file"
    normalized["campaign_id"] = _series_alias(working, ["Campaign ID", "CampaignId", "Campaign"])
    normalized["moderation_status"] = _series_alias(working, ["Moderation Status", "ModerationStatus", "Status"])
    normalized["brand_raw"] = _series_alias(working, ["Brand"])
    normalized["category_hierarchy"] = _series_alias(working, ["Category Hierarchy"])
    normalized["verified_purchaser"] = _series_alias(working, ["VerifiedPurchaser (CDV)", "Verified Purchaser", "Verified", "verification", "Verification"])

    seeded = _series_alias(working, ["Seeded Flag", "Seeded", "Incentivized", "IncentivizedReview (CDV)", "Incentivized Review", "IncentivizedReview", "Gifted", "Gifted Flag"])
    normalized["incentivized_review"] = seeded.map(
        lambda value: _parse_flag(
            value,
            pos=["seeded", "incentivized", "gifted", "sampled", "sampling", "sponsored", "paid", "yes", "true", "1", "t", "y"],
            neg=["organic", "organic standard", "organic / standard", "earned", "unpaid", "not seeded", "not incentivized", "not gifted", "no incentive", "no incentives", "standard", "no", "false", "0", "f", "n"],
        )
    )
    syndicated = _series_alias(working, ["Syndicated Flag", "Syndicated", "Syndicated Review", "Is Syndicated"])
    normalized["is_syndicated"] = syndicated.map(
        lambda value: _parse_flag(value, pos=["syndicated", "yes", "true", "1", "t", "y"], neg=["not syndicated", "no", "false", "0", "f", "n"])
    )

    normalized["photos_count"] = pd.to_numeric(normalized["photos_count"], errors="coerce").fillna(0)
    normalized["total_positive_feedback_count"] = pd.to_numeric(normalized["total_positive_feedback_count"], errors="coerce")
    normalized["is_recommended"] = normalized["is_recommended"].map(
        lambda value: _parse_flag(value, pos=["recommended", "yes", "true", "1", "t", "y"], neg=["not recommended", "no", "false", "0", "f", "n"])
    )
    normalized["verified_purchaser"] = normalized["verified_purchaser"].map(
        lambda value: _parse_flag(value, pos=["verified", "yes", "true", "1", "t", "y"], neg=["no", "false", "0", "f", "n"])
    )
    normalized["moderation_bucket"] = normalized["moderation_status"].map(_moderation_bucket)
    normalized["review_origin_group"] = [
        _review_origin_group_from_values(incent, syndicated_flag, campaign)
        for incent, syndicated_flag, campaign in zip(normalized["incentivized_review"], normalized["is_syndicated"], normalized["campaign_id"])
    ]
    normalized["review_acquisition_channel"] = [
        _review_acquisition_channel_from_values(incent, campaign)
        for incent, campaign in zip(normalized["incentivized_review"], normalized["campaign_id"])
    ]

    if include_local_symptomization:
        for col in _local_symptom_columns(list(working.columns)):
            normalized[col] = _normalize_symptom_series(working[col])
        for target, aliases in LOCAL_META_ALIASES.items():
            source = _pick_col(working, aliases)
            if source is not None:
                normalized[target] = working[source].astype("string").fillna("").str.strip().replace({"": pd.NA})

    existing_labels = {str(col).strip() for col in normalized.columns}
    existing_label_keys = {str(col).strip().lower() for col in normalized.columns}
    blocked_raw_columns = set()
    blocked_raw_keys = set()
    if not include_local_symptomization:
        blocked_raw_columns.update(_local_symptom_columns(list(working.columns)))
        blocked_raw_keys.update(str(col).strip().lower() for col in blocked_raw_columns)
    for raw_col in _all_dataframe_columns(working):
        raw_label = str(raw_col).strip()
        raw_key = raw_label.lower()
        if not raw_label or raw_label in existing_labels or raw_key in existing_label_keys or raw_label in blocked_raw_columns or raw_key in blocked_raw_keys:
            continue
        series = _select_column_series(working, raw_label)
        if series is None:
            continue
        normalized[raw_label] = series
        existing_labels.add(raw_label)
        existing_label_keys.add(raw_key)
    return finalize_df(normalized)


def read_uploaded_file(uploaded_file, *, include_local_symptomization: bool = False) -> pd.DataFrame:
    filename = getattr(uploaded_file, "name", "uploaded_file")
    raw = uploaded_file.getvalue()
    max_upload_mb = float(os.getenv("STARWALK_MAX_UPLOAD_MB", "40") or 40)
    max_upload_bytes = int(max_upload_mb * 1024 * 1024)
    if max_upload_bytes > 0 and len(raw) > max_upload_bytes:
        raise ValueError(f"{filename} exceeds the upload limit of {max_upload_mb:g} MB.")
    suffix = filename.lower().rsplit(".", 1)[-1] if "." in filename else "csv"
    if suffix == "csv":
        try:
            raw_df = pd.read_csv(io.BytesIO(raw))
        except UnicodeDecodeError:
            raw_df = pd.read_csv(io.BytesIO(raw), encoding="latin-1")
    elif suffix in {"xlsx", "xls", "xlsm"}:
        raw_df, sheet_name = _read_best_uploaded_excel_sheet(raw)
        raw_df.attrs["source_sheet_name"] = sheet_name
    else:
        raise ValueError(f"Unsupported file type: {filename}")
    if raw_df.empty:
        raise ValueError(f"{filename} is empty.")
    normalized = normalize_uploaded_df(raw_df, source_name=filename, include_local_symptomization=include_local_symptomization)
    source_sheet_name = raw_df.attrs.get("source_sheet_name")
    if source_sheet_name:
        normalized.attrs["source_sheet_name"] = source_sheet_name
    return normalized


def build_bv_dataset(
    raw_reviews: List[Dict[str, Any]],
    *,
    product_url: str,
    product_id: str,
    total: int,
    page_size: int,
    requests_needed: int,
    source_label: str,
    retailer: str = "",
    source_system: str = "Bazaarvoice",
):
    df = finalize_df(pd.DataFrame([flatten_bv_review(review) for review in raw_reviews]))
    if not df.empty:
        df["review_id"] = df["review_id"].astype(str)
        df["product_or_sku"] = df.get("product_or_sku", pd.Series(index=df.index, dtype="object")).fillna(product_id)
        df["base_sku"] = df.get("base_sku", pd.Series(index=df.index, dtype="object")).fillna(product_id)
        df["product_id"] = df["product_id"].fillna(product_id)
        df = apply_source_metadata(df, retailer=retailer, source_system=source_system, post_link=product_url)
    summary = ReviewBatchSummary(
        product_url=product_url,
        product_id=product_id,
        total_reviews=total,
        page_size=page_size,
        requests_needed=requests_needed,
        reviews_downloaded=len(df),
    )
    return {
        "summary": summary,
        "reviews_df": df,
        "source_type": "bazaarvoice",
        "source_label": source_label or product_url,
    }


def build_okendo_dataset(
    reviews: List[Dict[str, Any]],
    *,
    product_url: str,
    product_id: str,
    total: int,
    page_size: int,
    requests_needed: int,
    source_label: str,
    product_name: str = "",
    retailer: str = "",
    source_system: str = "Okendo",
):
    rows = [
        flatten_okendo_review(review, product_id=product_id, product_name=product_name, retailer=retailer, product_url=product_url)
        for review in reviews
    ]
    df = finalize_df(pd.DataFrame(rows))
    if not df.empty:
        df = apply_source_metadata(df, retailer=retailer, source_system=source_system, post_link=product_url)
    summary = ReviewBatchSummary(
        product_url=product_url,
        product_id=product_id,
        total_reviews=total,
        page_size=page_size,
        requests_needed=requests_needed,
        reviews_downloaded=len(df),
    )
    return {
        "summary": summary,
        "reviews_df": df,
        "source_type": "okendo",
        "source_label": source_label or product_url,
    }


def build_powerreviews_dataset(
    reviews: List[Dict[str, Any]],
    *,
    product_url: str,
    product_id: str,
    total: int,
    page_size: int,
    requests_needed: int,
    source_label: str,
    product_name: str = "",
    retailer: str = "",
    source_system: str = "PowerReviews",
):
    rows = [
        flatten_powerreviews_review(review, page_id=product_id, product_name=product_name, retailer=retailer, product_url=product_url)
        for review in reviews
    ]
    df = finalize_df(pd.DataFrame(rows))
    if not df.empty:
        df = apply_source_metadata(df, retailer=retailer, source_system=source_system, post_link=product_url)
    summary = ReviewBatchSummary(
        product_url=product_url,
        product_id=product_id,
        total_reviews=total,
        page_size=page_size,
        requests_needed=requests_needed,
        reviews_downloaded=len(df),
    )
    return {
        "summary": summary,
        "reviews_df": df,
        "source_type": "powerreviews",
        "source_label": source_label or product_url,
    }
