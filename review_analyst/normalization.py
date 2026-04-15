from __future__ import annotations

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
        df.loc[missing, "review_id"] = [f"review_{index + 1}" for index in range(int(missing.sum()))]

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
    fallback = df["base_sku"].where(df["base_sku"].ne(""), df["product_id"])
    df["product_or_sku"] = df["sku_item"].where(df["sku_item"].ne(""), fallback)
    df["product_or_sku"] = df["product_or_sku"].fillna("").astype(str).str.strip().replace({"": pd.NA})
    df["title_and_text"] = (df["title"].str.strip() + " " + df["review_text"].str.strip()).str.strip()
    df["has_photos"] = df["photos_count"] > 0
    df["has_media"] = df["has_photos"]
    df["review_length_chars"] = df["review_text"].str.len()
    df["review_length_words"] = df["review_text"].str.split().str.len().fillna(0).astype(int)
    df["rating_label"] = df["rating"].map(lambda x: f"{int(x)} star" if pd.notna(x) else "Unknown")
    df["year_month_sort"] = pd.to_datetime(df["submission_month"], format="%Y-%m", errors="coerce")
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


REVIEW_ID_ALIASES = ["Event Id", "Event ID", "Review ID", "Review Id", "Id", "review_id"]
REVIEW_TEXT_ALIASES = ["Review Text", "Review", "Body", "Content", "review_text"]
TITLE_ALIASES = ["Title", "Review Title", "Headline", "title"]
RATING_ALIASES = ["Rating (num)", "Rating", "Stars", "Star Rating", "rating"]
DATE_ALIASES = ["Opened date", "Opened Date", "Submission Time", "Review Date", "Date", "submission_time"]
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
    text = safe_text(value).lower()
    if text in {"", "nan", "none", "null", "n/a"}:
        return pd.NA
    if any(text == candidate.lower() for candidate in neg):
        return False
    if any(text == candidate.lower() for candidate in pos):
        return True
    if text.startswith(("not ", "non ")):
        return False
    return True


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
    normalized = pd.DataFrame(index=working.index)
    normalized["review_id"] = _series_alias(working, REVIEW_ID_ALIASES)
    normalized["product_id"] = _series_alias(working, ["Base SKU", "Product ID", "Product Id", "ProductId", "BaseSKU"])
    normalized["base_sku"] = _series_alias(working, ["Base SKU", "BaseSKU"])
    normalized["sku_item"] = _series_alias(working, ["SKU Item", "SKU", "Child SKU", "Variant SKU", "Item Number", "Item No"])
    normalized["original_product_name"] = _series_alias(working, ["Product Name", "Product", "Name"])
    normalized["review_text"] = _series_alias(working, REVIEW_TEXT_ALIASES)
    normalized["title"] = _series_alias(working, TITLE_ALIASES)
    normalized["post_link"] = _series_alias(working, ["Post Link", "URL", "Review URL", "Product URL"])
    normalized["rating"] = _series_alias(working, RATING_ALIASES)
    normalized["submission_time"] = _series_alias(working, DATE_ALIASES)
    normalized["content_locale"] = _series_alias(working, ["Content Locale", "Locale", "Location", "Country"])
    normalized["retailer"] = _series_alias(working, ["Retailer", "Merchant", "Channel"])
    normalized["age_group"] = _series_alias(working, ["Age Group", "Age", "Age Range"])
    normalized["user_location"] = _series_alias(working, ["Location", "Country"])
    normalized["user_nickname"] = pd.NA
    normalized["total_positive_feedback_count"] = pd.NA
    normalized["is_recommended"] = pd.NA
    normalized["photos_count"] = 0
    normalized["photo_urls"] = pd.NA
    normalized["source_file"] = source_name or pd.NA
    normalized["source_system"] = "Uploaded file"
    seeded = _series_alias(working, ["Seeded Flag", "Seeded", "Incentivized"])
    normalized["incentivized_review"] = seeded.map(
        lambda value: _parse_flag(value, pos=["seeded", "incentivized", "yes", "true", "1"], neg=["not seeded", "not incentivized", "no", "false", "0"])
    )
    syndicated = _series_alias(working, ["Syndicated Flag", "Syndicated"])
    normalized["is_syndicated"] = syndicated.map(
        lambda value: _parse_flag(value, pos=["syndicated", "yes", "true", "1"], neg=["not syndicated", "no", "false", "0"])
    )
    if include_local_symptomization:
        for col in _local_symptom_columns(list(working.columns)):
            normalized[col] = _normalize_symptom_series(working[col])
        for target, aliases in LOCAL_META_ALIASES.items():
            source = _pick_col(working, aliases)
            if source is not None:
                normalized[target] = working[source].astype("string").fillna("").str.strip().replace({"": pd.NA})
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
