"""Connector dispatch: review loading, BV/PR fetching, normalization

Extracted from app.py. Uses _app() for cross-module access.
"""
from __future__ import annotations
import json, math, re, sys, time
from typing import Any, Dict, List, Optional
from collections import namedtuple
import pandas as pd
import streamlit as st
try:
    import requests
except ImportError:
    requests = None

NON_VALUES = {"", "NA", "N/A", "NONE", "NULL", "NAN", "<NA>", "NOT MENTIONED"}

# ── Constants ──
DEFAULT_API_VERSION = "5.5"
DEFAULT_CONTENT_LOCALES = 'en_US,ar*,zh*,hr*,cs*,da*,nl*,en*,et*,fi*,fr*,de*,el*,he*,hu*,id*,it*,ja*,ko*,lv*,lt*,ms*,no*,pl*,pt*,ro*,sk*,sl*,es*,sv*,th*,tr*,vi*,en_AU,en_CA,en_GB'
DEFAULT_DISPLAYCODE = "15973_3_0-en_us"
DEFAULT_PAGE_SIZE = 100
DEFAULT_PASSKEY = "caC6wVBHos09eVeBkLIniLUTzrNMMH2XMADEhpHe1ewUw"
DEFAULT_SORT = "SubmissionTime:desc"

def _app():
    """Lazy access to app module namespace."""
    return sys.modules.get('__main__', sys.modules.get('app'))

def _esc(text):
    return html.escape(str(text or "")) if 'html' in dir() else str(text or "")

def _safe_text(value, default=""):
    if value is None: return default
    s = str(value).strip()
    return s if s else default

def _normalize_tag_list(tags):
    if not tags: return []
    seen = set()
    out = []
    for t in tags:
        s = str(t).strip()
        if not s or s.upper() in NON_VALUES: continue
        key = s.lower()
        if key not in seen: seen.add(key); out.append(s)
    return out


def _finalize_df(df):
    required = [
        "review_id", "product_id", "base_sku", "sku_item", "product_or_sku",
        "original_product_name", "title", "review_text", "rating", "is_recommended",
        "content_locale", "submission_time", "submission_date", "submission_month",
        "incentivized_review", "is_syndicated", "photos_count", "photo_urls",
        "title_and_text", "retailer", "post_link", "age_group", "user_nickname",
        "user_location", "total_positive_feedback_count", "source_system", "source_file",
    ]
    df = _app()._ensure_cols(df.copy(), required)
    if df.empty:
        for c in ["has_photos", "has_media", "review_length_chars", "review_length_words", "rating_label", "year_month_sort"]:
            if c not in df.columns:
                df[c] = pd.Series(dtype="object")
        return df

    df["review_id"] = df["review_id"].fillna("").astype(str).str.strip()
    missing = df["review_id"].eq("") | df["review_id"].str.lower().isin({"nan", "none", "null"})
    if missing.any():
        df.loc[missing, "review_id"] = [f"review_{i + 1}" for i in range(int(missing.sum()))]
    if "context_data_json" in df.columns:
        df["age_group"] = df["age_group"].fillna(df["context_data_json"].map(_extract_age_group))
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
    sc = [c for c in ["submission_time", "review_id"] if c in df.columns]
    if sc:
        df = df.sort_values(sc, ascending=[False, False], na_position="last").reset_index(drop=True)
    return df



def _flatten_review(r):
    photos = r.get("Photos") or []
    urls = []
    for p in photos:
        sz = p.get("Sizes") or {}
        for sn in ["large", "normal", "thumbnail"]:
            u = (sz.get(sn) or {}).get("Url")
            if u:
                urls.append(u)
                break
    syn = r.get("SyndicationSource") or {}
    return dict(
        review_id=r.get("Id"),
        product_id=r.get("ProductId"),
        original_product_name=r.get("OriginalProductName"),
        title=_safe_text(r.get("Title")),
        review_text=_safe_text(r.get("ReviewText")),
        rating=r.get("Rating"),
        is_recommended=r.get("IsRecommended"),
        user_nickname=r.get("UserNickname"),
        author_id=r.get("AuthorId"),
        user_location=r.get("UserLocation"),
        content_locale=r.get("ContentLocale"),
        submission_time=r.get("SubmissionTime"),
        moderation_status=r.get("ModerationStatus"),
        campaign_id=r.get("CampaignId"),
        source_client=r.get("SourceClient"),
        is_featured=r.get("IsFeatured"),
        is_syndicated=r.get("IsSyndicated"),
        syndication_source_name=syn.get("Name"),
        is_ratings_only=r.get("IsRatingsOnly"),
        total_positive_feedback_count=r.get("TotalPositiveFeedbackCount"),
        badges=", ".join(str(x) for x in (r.get("BadgesOrder") or [])),
        context_data_json=json.dumps(r.get("ContextDataValues") or {}, ensure_ascii=False),
        photos_count=len(photos),
        photo_urls=" | ".join(urls),
        incentivized_review=_app()._is_incentivized(r),
        raw_json=json.dumps(r, ensure_ascii=False),
    )



def _flatten_powerreviews_review(review, *, page_id="", product_name="", retailer="", product_url=""):
    details = review.get("details") or {}
    metrics = review.get("metrics") or {}
    media = review.get("media") or []
    photo_urls = _app()._powerreviews_media_urls(media)
    product_id = _safe_text(details.get("product_page_id") or review.get("page_id") or page_id)

    incentivized = False
    badges = review.get("badges") or {}
    for key, val in badges.items():
        lk = str(key).lower()
        if any(tok in lk for tok in ["sampling", "sample", "sweepstakes", "incentivized", "influencer"]):
            if bool(val):
                incentivized = True
                break

    return dict(
        review_id=review.get("review_id") or review.get("ugc_id") or review.get("internal_review_id") or review.get("legacy_id"),
        product_id=product_id,
        base_sku=product_id,
        sku_item=product_id,
        original_product_name=_safe_text(product_name),
        title=_safe_text(details.get("headline") or review.get("headline")),
        review_text=_safe_text(details.get("comments") or review.get("comments")),
        rating=metrics.get("rating") or details.get("rating"),
        is_recommended=_app()._powerreviews_bool_from_bottom_line(details.get("bottom_line") or review.get("bottom_line")),
        user_nickname=_safe_text(details.get("nickname") or review.get("nickname")),
        author_id=review.get("author_id"),
        user_location=_safe_text(details.get("location") or review.get("location")),
        content_locale=_safe_text(details.get("locale") or review.get("locale")),
        submission_time=_app()._powerreviews_submission_iso(details.get("created_date") or review.get("created_date")),
        moderation_status=review.get("status") or pd.NA,
        campaign_id=pd.NA,
        source_client=pd.NA,
        is_featured=pd.NA,
        is_syndicated=bool(review.get("is_syndicated") or badges.get("is_syndicated") or False),
        syndication_source_name=pd.NA,
        is_ratings_only=False,
        total_positive_feedback_count=metrics.get("helpful_votes"),
        badges=", ".join([str(k) for k, v in (badges or {}).items() if bool(v)]),
        context_data_json=json.dumps(details.get("properties") or [], ensure_ascii=False),
        photos_count=len(photo_urls),
        photo_urls=" | ".join(photo_urls),
        incentivized_review=incentivized,
        raw_json=json.dumps(review, ensure_ascii=False),
        retailer=retailer,
        post_link=product_url,
        source_system="PowerReviews",
    )



def _normalize_uploaded_df(raw, *, source_name="", include_local_symptomization=False):
    w = raw.copy()
    w.columns = [str(c).strip() for c in w.columns]
    n = pd.DataFrame(index=w.index)
    n["review_id"] = _app()._series_alias(w, _app().UPLOAD_REVIEW_ID_ALIASES)
    n["product_id"] = _app()._series_alias(w, ["Product ID", "Product Id", "ProductId", "Base SKU", "BaseSKU"])
    n["base_sku"] = _app()._series_alias(w, ["Base SKU", "BaseSKU"])
    n["sku_item"] = _app()._series_alias(w, ["SKU Item", "SKU", "Child SKU", "Variant SKU", "Item Number", "Item No"])
    n["original_product_name"] = _app()._series_alias(w, ["Product Name", "Product", "Name"])
    n["review_text"] = _app()._series_alias(w, _app().UPLOAD_REVIEW_TEXT_ALIASES)
    n["title"] = _app()._series_alias(w, _app().UPLOAD_TITLE_ALIASES)
    n["post_link"] = _app()._series_alias(w, ["Post Link", "URL", "Review URL", "Product URL", "Product Page URL"])
    n["rating"] = _app()._series_alias(w, _app().UPLOAD_RATING_ALIASES)
    n["submission_time"] = _app()._series_alias(w, _app().UPLOAD_DATE_ALIASES)
    n["content_locale"] = _app()._series_alias(w, ["Review Display Locale", "Content Locale", "Locale", "Location", "Country"])
    n["retailer"] = _app()._series_alias(w, ["Retailer", "Merchant", "Channel"])
    n["age_group"] = _app()._series_alias(w, ["Age Group", "Age", "Age Range"])
    n["user_location"] = _app()._series_alias(w, ["Location", "Country"])
    n["user_nickname"] = pd.NA
    n["total_positive_feedback_count"] = pd.NA
    n["is_recommended"] = pd.NA
    n["photos_count"] = 0
    n["photo_urls"] = pd.NA
    n["source_file"] = source_name or pd.NA
    n["source_system"] = "Uploaded file"
    seeded = _app()._series_alias(w, ["Seeded Flag", "Seeded", "Incentivized"])
    n["incentivized_review"] = seeded.map(lambda v: _app()._parse_flag(v,
        pos=["seeded", "incentivized", "yes", "true", "1"],
        neg=["not seeded", "not incentivized", "no", "false", "0"]))
    syndicated = _app()._series_alias(w, ["Syndicated Flag", "Syndicated"])
    n["is_syndicated"] = syndicated.map(lambda v: _app()._parse_flag(v,
        pos=["syndicated", "yes", "true", "1"],
        neg=["not syndicated", "no", "false", "0"]))
    if include_local_symptomization:
        for col in _app()._local_symptom_columns(list(w.columns)):
            n[col] = _app()._normalize_symptom_series(w[col])
        for target, aliases in _app().LOCAL_SYMPTOM_META_ALIASES.items():
            source = _app()._pick_col(w, aliases)
            if source is not None:
                n[target] = w[source].astype("string").fillna("").str.strip().replace({"": pd.NA})
    return _finalize_df(n)



def _extract_candidate_tokens_from_html(html: str) -> List[str]:
    if not html:
        return []
    text = html.replace(r"\/", "/")
    soup = BeautifulSoup(html, "html.parser")
    visible = soup.get_text(" ", strip=True)
    cands: List[str] = []

    patterns = [
        r'Item\s*No\.?\s*([A-Za-z0-9_-]{4,40})',
        r'Item\s*([A-Za-z0-9_-]{4,40})',
        r'"productId"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'"product_id"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'"ProductId"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'"page_id"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'"product_page_id"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'"masterId"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'"styleNumber"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'data-product-id=["\']([^"\']+)',
        r'data-sku=["\']([^"\']+)',
        r'display\.powerreviews\.com/m/\d+/l/[A-Za-z_]+/product/([^/\"\'&?<>]+)/reviews',
        r'api\.bazaarvoice\.com/data/reviews\.json[^\"\']*productid(?::eq)?:([^,&\"\']+)',
        r'\b(P\d{5,10})\b',
        r'\b(pimprod\d{5,12})\b',
        r'\b(xlsImpprod\d{5,12})\b',
    ]
    for pat in patterns:
        for match in re.findall(pat, text, flags=re.IGNORECASE):
            cands.append(match)
        for match in re.findall(pat, visible, flags=re.IGNORECASE):
            cands.append(match)

    # Look for numeric or SKU-like ids near review / powerreviews / bazaarvoice references.
    windows = re.findall(r"(?i)(.{0,140}(?:powerreviews|bazaarvoice|reviews|review snapshot).{0,240})", text)
    for window in windows:
        cands.extend(re.findall(r"\b([A-Za-z]*\d[A-Za-z0-9_-]{3,30})\b", window))

    out = []
    for raw in cands:
        tok = _app()._safe_candidate_token(raw)
        if tok:
            out.append(tok)
    return _app()._dedupe_keep_order(out)



def _probe_bazaarvoice_candidates(session, *, product_url: str, candidates: Sequence[str], cfg: Dict[str, Any]):
    tried = []
    zero_match = None
    for candidate in _app()._dedupe_keep_order(candidates):
        try:
            if cfg.get("kind") == "action":
                payload = _app()._fetch_reviews_page(
                    session,
                    product_id=candidate,
                    passkey=cfg["passkey"],
                    displaycode=cfg["displaycode"],
                    api_version=cfg.get("api_version", DEFAULT_API_VERSION),
                    page_size=1,
                    offset=0,
                    sort=cfg.get("sort", DEFAULT_SORT),
                    content_locales=cfg.get("content_locales", DEFAULT_CONTENT_LOCALES),
                )
            else:
                payload = _app()._fetch_bv_simple_page(
                    session,
                    product_id=candidate,
                    passkey=cfg["passkey"],
                    api_version=cfg.get("api_version", "5.4"),
                    page_size=1,
                    offset=0,
                    sort=cfg.get("sort", "SubmissionTime:desc"),
                    content_locale=cfg.get("content_locale", ""),
                    locale=cfg.get("locale", "en_US"),
                    include=cfg.get("include", "Products,Comments"),
                )
            total = int(payload.get("TotalResults", 0) or 0)
            has_products = bool(((payload.get("Includes") or {}).get("Products") or {}))
            if total > 0 or has_products:
                return candidate, payload
            if zero_match is None:
                zero_match = (candidate, payload)
        except Exception as exc:
            tried.append(f"{candidate}: {exc}")
    if zero_match is not None:
        return zero_match
    raise ReviewDownloaderError("Could not match a Bazaarvoice product ID. Tried: " + "; ".join(tried[:8]))



def _load_powerreviews_api_url(api_url: str, *, product_url_hint: str = "", retailer_hint: str = ""):
    session = _app()._get_session()
    parsed = urlparse(api_url)
    m = re.search(r"/m/(\d+)/l/([A-Za-z_]+)/product/([^/]+)/reviews", parsed.path)
    if not m:
        raise ReviewDownloaderError("Could not parse PowerReviews API URL.")
    merchant_id, locale, product_id = m.groups()
    params = parse_qs(parsed.query, keep_blank_values=True)
    apikey = _app()._query_first_ci(params, ["apikey"])
    if not apikey:
        raise ReviewDownloaderError("PowerReviews API URL missing apikey.")
    sort = _app()._query_first_ci(params, ["sort"], default="Newest") or "Newest"
    page_size = min(int(_app()._query_first_ci(params, ["paging.size"], default=_app().POWERREVIEWS_MAX_PAGE_SIZE) or _app().POWERREVIEWS_MAX_PAGE_SIZE), _app().POWERREVIEWS_MAX_PAGE_SIZE)
    first = _app()._fetch_powerreviews_page(
        session,
        merchant_id=merchant_id,
        locale=locale,
        product_id=product_id,
        apikey=apikey,
        paging_from=0,
        page_size=page_size,
        sort=sort,
    )
    paging = first.get("paging") or {}
    total = int(paging.get("total_results", 0) or 0)
    results = first.get("results") or []
    product_name = _safe_text((results[0].get("rollup") or {}).get("name") if results else "")
    all_reviews: List[Dict[str, Any]] = []
    for result in results:
        all_reviews.extend(result.get("reviews") or [])
    if total > len(all_reviews):
        offsets = list(range(len(all_reviews), total, page_size))
        progress = st.progress(0.0, text="Downloading…")
        for i, start in enumerate(offsets, 1):
            payload = _app()._fetch_powerreviews_page(
                session,
                merchant_id=merchant_id,
                locale=locale,
                product_id=product_id,
                apikey=apikey,
                paging_from=int(start),
                page_size=page_size,
                sort=sort,
            )
            for result in payload.get("results") or []:
                all_reviews.extend(result.get("reviews") or [])
            progress.progress(i / max(len(offsets), 1))
    source_label = product_url_hint or api_url
    return _app()._build_powerreviews_dataset(
        all_reviews,
        product_url=product_url_hint or api_url,
        product_id=product_id,
        total=total,
        page_size=page_size,
        requests_needed=max(1, math.ceil(total / max(page_size, 1))) if total else 1,
        source_label=source_label,
        product_name=product_name,
        retailer=retailer_hint,
        source_system="PowerReviews API",
    )



def _fetch_all_bazaarvoice_for_candidate(session, *, product_url: str, product_id: str, cfg: Dict[str, Any]):
    if cfg.get("kind") == "action":
        first = _app()._fetch_reviews_page(
            session,
            product_id=product_id,
            passkey=cfg["passkey"],
            displaycode=cfg["displaycode"],
            api_version=cfg.get("api_version", DEFAULT_API_VERSION),
            page_size=DEFAULT_PAGE_SIZE,
            offset=0,
            sort=cfg.get("sort", DEFAULT_SORT),
            content_locales=cfg.get("content_locales", DEFAULT_CONTENT_LOCALES),
        )
        total = int(first.get("TotalResults", 0) or 0)
        raw_reviews = list(first.get("Results") or [])
        offsets = list(range(len(raw_reviews), total, DEFAULT_PAGE_SIZE))
        progress = st.progress(0.0, text="Downloading…") if offsets else None
        for i, offset in enumerate(offsets, 1):
            page = _app()._fetch_reviews_page(
                session,
                product_id=product_id,
                passkey=cfg["passkey"],
                displaycode=cfg["displaycode"],
                api_version=cfg.get("api_version", DEFAULT_API_VERSION),
                page_size=DEFAULT_PAGE_SIZE,
                offset=offset,
                sort=cfg.get("sort", DEFAULT_SORT),
                content_locales=cfg.get("content_locales", DEFAULT_CONTENT_LOCALES),
            )
            raw_reviews.extend(page.get("Results") or [])
            if progress is not None:
                progress.progress(i / max(len(offsets), 1))
        return _app()._build_bv_dataset(
            raw_reviews,
            product_url=product_url,
            product_id=product_id,
            total=total,
            page_size=DEFAULT_PAGE_SIZE,
            requests_needed=max(1, math.ceil(total / DEFAULT_PAGE_SIZE)) if total else 1,
            source_label=product_url,
            retailer=cfg.get("retailer", ""),
            source_system=cfg.get("source_system", "Bazaarvoice"),
        )

    first = _app()._fetch_bv_simple_page(
        session,
        product_id=product_id,
        passkey=cfg["passkey"],
        api_version=cfg.get("api_version", "5.4"),
        page_size=DEFAULT_PAGE_SIZE,
        offset=0,
        sort=cfg.get("sort", "SubmissionTime:desc"),
        content_locale=cfg.get("content_locale", ""),
        locale=cfg.get("locale", "en_US"),
        include=cfg.get("include", "Products,Comments"),
    )
    total = int(first.get("TotalResults", 0) or 0)
    raw_reviews = list(first.get("Results") or [])
    offsets = list(range(len(raw_reviews), total, DEFAULT_PAGE_SIZE))
    progress = st.progress(0.0, text="Downloading…") if offsets else None
    for i, offset in enumerate(offsets, 1):
        page = _app()._fetch_bv_simple_page(
            session,
            product_id=product_id,
            passkey=cfg["passkey"],
            api_version=cfg.get("api_version", "5.4"),
            page_size=DEFAULT_PAGE_SIZE,
            offset=offset,
            sort=cfg.get("sort", "SubmissionTime:desc"),
            content_locale=cfg.get("content_locale", ""),
            locale=cfg.get("locale", "en_US"),
            include=cfg.get("include", "Products,Comments"),
        )
        raw_reviews.extend(page.get("Results") or [])
        if progress is not None:
            progress.progress(i / max(len(offsets), 1))
    return _app()._build_bv_dataset(
        raw_reviews,
        product_url=product_url,
        product_id=product_id,
        total=total,
        page_size=DEFAULT_PAGE_SIZE,
        requests_needed=max(1, math.ceil(total / DEFAULT_PAGE_SIZE)) if total else 1,
        source_label=product_url,
        retailer=cfg.get("retailer", ""),
        source_system=cfg.get("source_system", "Bazaarvoice"),
    )



def _fetch_all_powerreviews_for_candidate(session, *, product_url: str, product_id: str, cfg: Dict[str, Any]):
    page_size = _app().POWERREVIEWS_MAX_PAGE_SIZE
    first = _app()._fetch_powerreviews_page(
        session,
        merchant_id=cfg["merchant_id"],
        locale=cfg.get("locale", "en_US"),
        product_id=product_id,
        apikey=cfg["apikey"],
        paging_from=0,
        page_size=page_size,
        sort=cfg.get("sort", "Newest"),
    )
    paging = first.get("paging") or {}
    total = int(paging.get("total_results", 0) or 0)
    results = first.get("results") or []
    product_name = _safe_text((results[0].get("rollup") or {}).get("name") if results else "")
    all_reviews: List[Dict[str, Any]] = []
    for result in results:
        all_reviews.extend(result.get("reviews") or [])
    offsets = list(range(len(all_reviews), total, page_size))
    progress = st.progress(0.0, text="Downloading…") if offsets else None
    for i, start in enumerate(offsets, 1):
        payload = _app()._fetch_powerreviews_page(
            session,
            merchant_id=cfg["merchant_id"],
            locale=cfg.get("locale", "en_US"),
            product_id=product_id,
            apikey=cfg["apikey"],
            paging_from=int(start),
            page_size=page_size,
            sort=cfg.get("sort", "Newest"),
        )
        for result in payload.get("results") or []:
            all_reviews.extend(result.get("reviews") or [])
        if progress is not None:
            progress.progress(i / max(len(offsets), 1))
    return _app()._build_powerreviews_dataset(
        all_reviews,
        product_url=product_url,
        product_id=product_id,
        total=total,
        page_size=page_size,
        requests_needed=max(1, math.ceil(total / page_size)) if total else 1,
        source_label=product_url,
        product_name=product_name,
        retailer=cfg.get("retailer", ""),
        source_system=cfg.get("source_system", "PowerReviews"),
    )



def _load_product_reviews(product_url):
    product_url = _app()._normalize_input_url(product_url)
    parsed = urlparse(product_url)
    host = _app()._strip_www(parsed.netloc)
    retailer_hint = ""
    if "costco.com" in host:
        retailer_hint = "Costco"
    elif "sephora.com" in host:
        retailer_hint = "Sephora"
    elif "ulta.com" in host:
        retailer_hint = "Ulta"
    elif "hoka.com" in host:
        retailer_hint = "Hoka"
    elif _app()._looks_like_sharkninja_uk_eu(host):
        retailer_hint = "SharkNinja UK/EU"
    elif _app()._looks_like_sharkninja_us(host):
        retailer_hint = "SharkNinja"

    if _app()._is_bazaarvoice_api_url(product_url):
        return _app()._load_bazaarvoice_api_url(product_url)
    if _app()._is_powerreviews_api_url(product_url):
        return _load_powerreviews_api_url(product_url)

    session = _app()._get_session()
    product_html = ""
    page_fetch_error = None
    with st.spinner("Loading product page…"):
        try:
            resp = session.get(product_url, timeout=35)
            resp.raise_for_status()
            product_html = resp.text or ""
        except Exception as exc:
            page_fetch_error = exc
            product_html = ""

    # First: if the page source embeds a review API URL, use that directly.
    embedded_urls = _app()._extract_embedded_review_api_urls(product_html) if product_html else []
    if embedded_urls:
        for api_url in embedded_urls:
            try:
                if _app()._is_bazaarvoice_api_url(api_url):
                    return _app()._load_bazaarvoice_api_url(api_url, product_url_hint=product_url, retailer_hint=retailer_hint)
                if _app()._is_powerreviews_api_url(api_url):
                    return _load_powerreviews_api_url(api_url, product_url_hint=product_url, retailer_hint=retailer_hint)
            except Exception:
                continue

    # Site-specific fallbacks that can work even when the retailer blocks page scraping.
    if "costco.com" in host:
        return _app()._load_bazaarvoice_product_page(
            session,
            product_url=product_url,
            product_html=product_html,
            cfg={**_app().COSTCO_BV_CONFIG, "kind": "action", "source_system": "Bazaarvoice", "retailer": "Costco"},
        )

    if "sephora.com" in host:
        sephora_candidates = []
        sephora_candidates.extend(re.findall(r"\b(P\d{5,10})\b", product_url, flags=re.IGNORECASE))
        sephora_candidates.extend(re.findall(r"\b(P\d{5,10})\b", product_html or "", flags=re.IGNORECASE))
        return _app()._load_bazaarvoice_product_page(
            session,
            product_url=product_url,
            product_html=product_html,
            cfg={**_app().SEPHORA_BV_CONFIG, "kind": "simple", "content_locale": "en*", "source_system": "Bazaarvoice", "retailer": "Sephora"},
            extra_candidates=sephora_candidates,
        )

    if "ulta.com" in host:
        ulta_candidates = []
        q = parse_qs(parsed.query)
        ulta_candidates.extend(q.get("sku", []))
        ulta_candidates.extend(re.findall(r"\b(pimprod\d{5,12})\b", product_html or "", flags=re.IGNORECASE))
        ulta_candidates.extend(re.findall(r"\b(pimprod\d{5,12})\b", product_url, flags=re.IGNORECASE))
        ulta_candidates.extend(re.findall(r"\b(xlsImpprod\d{5,12})\b", product_url, flags=re.IGNORECASE))
        return _app()._load_powerreviews_product_page(
            session,
            product_url=product_url,
            product_html=product_html,
            cfg={**_app().ULTA_PR_CONFIG, "source_system": "PowerReviews"},
            extra_candidates=ulta_candidates,
        )

    if "hoka.com" in host:
        hoka_candidates = []
        hoka_candidates.extend(re.findall(r"/(\d+)\.html", parsed.path))
        hoka_candidates.extend(re.findall(r"dwvar_(\d+)_", product_url))
        hoka_candidates.extend(re.findall(r"Item\s*No\.?\s*(\d{5,10})", product_html or "", flags=re.IGNORECASE))
        hoka_candidates.extend(re.findall(r'"product_page_id"\s*[:=]\s*"?(\d{5,10})', product_html or "", flags=re.IGNORECASE))
        hoka_candidates.extend(re.findall(r'"page_id"\s*[:=]\s*"?(\d{5,10})', product_html or "", flags=re.IGNORECASE))
        return _app()._load_powerreviews_product_page(
            session,
            product_url=product_url,
            product_html=product_html,
            cfg={**_app().HOKA_PR_CONFIG, "source_system": "PowerReviews"},
            extra_candidates=hoka_candidates,
        )

    if _app()._looks_like_sharkninja_uk_eu(host):
        uk_pid = _app()._extract_generic_bv_product_id(product_url, product_html)
        return _app()._load_bazaarvoice_product_page(
            session,
            product_url=product_url,
            product_html=product_html,
            cfg={**_app().SHARKNINJA_UK_EU_BV_CONFIG, "kind": "simple", "source_system": "Bazaarvoice"},
            extra_candidates=[uk_pid] if uk_pid else None,
        )

    # Default SharkNinja US / generic Bazaarvoice product pages.
    pid = _app()._extract_pid_from_url(product_url) or _app()._extract_pid_from_html(product_html)
    if pid:
        try:
            return _fetch_all_bazaarvoice_for_candidate(
                session,
                product_url=product_url,
                product_id=pid,
                cfg={
                    "kind": "action",
                    "passkey": DEFAULT_PASSKEY,
                    "displaycode": DEFAULT_DISPLAYCODE,
                    "api_version": DEFAULT_API_VERSION,
                    "sort": DEFAULT_SORT,
                    "content_locales": DEFAULT_CONTENT_LOCALES,
                    "retailer": "SharkNinja",
                    "source_system": "Bazaarvoice",
                },
            )
        except Exception:
            pass

    # Last chance: generic embedded/candidate Bazaarvoice probes.
    generic_candidates = []
    generic_candidates.extend(_app()._extract_candidate_tokens_from_url(product_url))
    generic_candidates.extend(_extract_candidate_tokens_from_html(product_html))
    try:
        return _app()._load_bazaarvoice_product_page(
            session,
            product_url=product_url,
            product_html=product_html,
            cfg={
                "kind": "action",
                "passkey": DEFAULT_PASSKEY,
                "displaycode": DEFAULT_DISPLAYCODE,
                "api_version": DEFAULT_API_VERSION,
                "sort": DEFAULT_SORT,
                "content_locales": DEFAULT_CONTENT_LOCALES,
                "retailer": "SharkNinja",
                "source_system": "Bazaarvoice",
            },
            extra_candidates=generic_candidates,
        )
    except Exception:
        if page_fetch_error is not None:
            if isinstance(page_fetch_error, requests.HTTPError):
                raise
            raise ReviewDownloaderError(f"Could not load product page or match a review feed: {page_fetch_error}")
        raise



def _load_product_reviews_dispatch(product_url: str):
    if _app()._use_package_connectors() and callable(_package_load_product_reviews):
        return _package_load_product_reviews(product_url)
    return _load_product_reviews(product_url)



def _load_multiple_product_reviews(urls: Sequence[str]):
    if isinstance(urls, str):
        url_list = _app()._parse_bulk_product_urls(urls)
    else:
        url_list = _app()._parse_bulk_product_urls("\n".join([str(u) for u in (urls or [])]))
    if not url_list:
        raise ReviewDownloaderError("Add at least one product or review URL.")
    if len(url_list) == 1:
        return _load_product_reviews(url_list[0])

    progress = st.progress(0.0, text="Preparing multi-link load…")
    status = st.empty()
    frames: List[pd.DataFrame] = []
    loaded: List[Dict[str, Any]] = []
    failures: List[Tuple[str, str]] = []

    for i, url in enumerate(url_list, start=1):
        status.info(f"Loading {i}/{len(url_list)} · {url}")
        try:
            ds = _load_product_reviews(url)
            frame = ds["reviews_df"].copy()
            frame["loaded_from_url"] = url
            frame["loaded_from_host"] = _app()._strip_www(urlparse(url).netloc) or "Unknown"
            frame["loaded_from_label"] = _safe_text(ds.get("source_label")) or url
            frame["loaded_from_batch"] = f"Link {i}"
            frames.append(frame)
            loaded.append(ds)
        except Exception as exc:
            failures.append((url, str(exc)))
        progress.progress(i / len(url_list), text=f"Loaded {len(loaded)} of {len(url_list)} links")

    if not frames:
        details = "; ".join(f"{u} → {err}" for u, err in failures[:3]) if failures else "No links loaded."
        raise ReviewDownloaderError(f"Could not load any links. {details}")

    combined = pd.concat(frames, ignore_index=True)
    combined = _app()._dedupe_combined_reviews(combined)
    combined = _finalize_df(combined)

    summary = ReviewBatchSummary(
        product_url="\n".join(url_list),
        product_id=f"MULTI_URL_WORKSPACE_{len(url_list)}",
        total_reviews=len(combined),
        page_size=max(len(combined), 1),
        requests_needed=sum(int(getattr(ds.get("summary"), "requests_needed", 0)) for ds in loaded),
        reviews_downloaded=len(combined),
    )
    status.success(f"Loaded {len(combined):,} reviews from {len(loaded)} link(s).")
    if failures:
        st.warning("Some links could not be loaded: " + " | ".join(f"{u} → {err}" for u, err in failures[:3]))
    return dict(
        summary=summary,
        reviews_df=combined,
        source_type="multi-url",
        source_label=_app()._format_multi_source_label(url_list),
        source_urls=url_list,
        source_failures=failures,
    )


