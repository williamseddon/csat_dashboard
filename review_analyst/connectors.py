from __future__ import annotations

import math
import re
from collections import Counter
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import pandas as pd
import requests

from .config import (
    BAZAARVOICE_ENDPOINT,
    DEFAULT_API_VERSION,
    DEFAULT_CONTENT_LOCALES,
    DEFAULT_DISPLAYCODE,
    DEFAULT_PAGE_SIZE,
    DEFAULT_PASSKEY,
    DEFAULT_SORT,
    OKENDO_API_ROOT,
    OKENDO_API_VERSION,
    OKENDO_MAX_PAGE_SIZE,
    POWERREVIEWS_ENDPOINT_TEMPLATE,
    POWERREVIEWS_MAX_PAGE_SIZE,
    SITE_REVIEW_CONFIGS,
)
from .models import LoadedWorkspace, ReviewBatchSummary, ReviewDownloaderError
from .normalization import (
    build_bv_dataset,
    build_okendo_dataset,
    build_powerreviews_dataset,
    finalize_df,
    read_uploaded_file,
)
from .utils import (
    dedupe_keep_order,
    domain_matches,
    extract_candidate_tokens_from_html,
    extract_candidate_tokens_from_url,
    extract_embedded_review_api_urls,
    format_multi_source_label,
    host_matches,
    is_bazaarvoice_api_url,
    is_okendo_api_url,
    is_powerreviews_api_url,
    looks_like_sharkninja_uk_eu,
    looks_like_sharkninja_us,
    normalize_input_url,
    ordered_unique,
    parse_bulk_product_urls,
    safe_text,
    strip_www,
)


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Upgrade-Insecure-Requests": "1",
        }
    )
    return session


def _report_progress(progress_ui: Optional[Callable[..., None]], *, progress: Optional[float] = None, title: str = "", detail: str = "") -> None:
    if not callable(progress_ui):
        return
    kwargs: Dict[str, Any] = {}
    if progress is not None:
        kwargs["progress"] = progress
    if title:
        kwargs["title"] = safe_text(title)
    if detail:
        kwargs["detail"] = safe_text(detail)
    try:
        progress_ui(**kwargs)
    except TypeError:
        try:
            progress_ui(progress, safe_text(title), safe_text(detail))
        except Exception:
            return
    except Exception:
        return


def _scale_progress(progress_ui: Optional[Callable[..., None]], *, start: float = 0.0, end: float = 1.0, prefix: str = ""):
    if not callable(progress_ui):
        return None
    start_f = float(start or 0.0)
    end_f = float(end or 0.0)

    def _child(*, progress: Optional[float] = None, title: str = "", detail: str = "") -> None:
        mapped = None
        if progress is not None:
            try:
                base = max(0.0, min(1.0, float(progress)))
            except Exception:
                base = 0.0
            mapped = start_f + (end_f - start_f) * base
        title_text = safe_text(title).strip()
        if prefix and title_text:
            title_text = f"{prefix} · {title_text}"
        elif prefix:
            title_text = prefix
        _report_progress(progress_ui, progress=mapped, title=title_text, detail=detail)

    return _child


def site_config_from_url(url: str) -> Optional[Dict[str, Any]]:
    host = urlparse(safe_text(url)).netloc.lower()
    for cfg in SITE_REVIEW_CONFIGS:
        if any(domain_matches(host, domain) for domain in cfg.get("domains", [])):
            return dict(cfg)
    return None


def _query_first_ci(mapping: Dict[str, Any], candidates: Sequence[str], default: Any = None) -> Any:
    wanted = {str(candidate).lower() for candidate in candidates}
    for key, value in (mapping or {}).items():
        if str(key).lower() in wanted:
            if isinstance(value, list):
                return value[0] if value else default
            return value if value not in (None, "") else default
    return default


def _set_ci_param(mapping: Dict[str, Any], candidates: Sequence[str], value: Any) -> str:
    wanted = {str(candidate).lower() for candidate in candidates}
    for key in list(mapping.keys()):
        if str(key).lower() in wanted:
            mapping[key] = value
            return key
    key = list(candidates)[0]
    mapping[key] = value
    return key


def _clone_params(mapping: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, value in mapping.items():
        out[key] = list(value) if isinstance(value, list) else value
    return out


def _extract_bazaarvoice_product_id_from_params(params: Dict[str, Any]) -> Optional[str]:
    direct = _query_first_ci(params, ["productid", "productId", "ProductId", "pid", "id"])
    if direct:
        return safe_text(direct)
    for key, values in params.items():
        if str(key).lower() not in {"filter", "filter_reviews", "filterreviews"}:
            continue
        values_list = values if isinstance(values, list) else [values]
        for value in values_list:
            match = re.search(r"productid(?::eq)?:([^,&]+)", str(value), flags=re.IGNORECASE)
            if match:
                return safe_text(match.group(1))
    return None


def _extract_generic_bv_product_id(url: str, html_text: str) -> Optional[str]:
    parsed = urlparse(url.strip())
    params = parse_qs(parsed.query)
    for key in ["productId", "product_id", "itemId", "item_id", "pid", "id", "sku"]:
        if key in params and params[key]:
            return safe_text(params[key][0])
    zid_match = re.search(r"[_\-]zid([A-Z0-9\-_]+)$", parsed.path, re.IGNORECASE)
    if zid_match:
        return safe_text(zid_match.group(1))
    html_candidates = extract_candidate_tokens_from_html(html_text)
    for token in html_candidates:
        if re.fullmatch(r"[A-Za-z0-9_-]{4,40}", token):
            return token
    segments = [segment for segment in parsed.path.split("/") if segment]
    if segments:
        last = re.sub(r"\.html?$", "", segments[-1], flags=re.IGNORECASE)
        trailing = re.search(r"([A-Z0-9]{4,20})$", last, re.IGNORECASE)
        if trailing:
            return trailing.group(1)
        return safe_text(last)
    return None


def fetch_reviews_page(
    session: requests.Session,
    *,
    product_id: str,
    passkey: str,
    displaycode: str,
    api_version: str,
    page_size: int,
    offset: int,
    sort: str,
    content_locales: str,
) -> Dict[str, Any]:
    params = dict(
        resource="reviews",
        action="REVIEWS_N_STATS",
        filter=[
            f"productid:eq:{product_id}",
            f"contentlocale:eq:{content_locales}",
            "isratingsonly:eq:false",
        ],
        filter_reviews=f"contentlocale:eq:{content_locales}",
        include="authors,products,comments",
        filteredstats="reviews",
        Stats="Reviews",
        limit=int(page_size),
        offset=int(offset),
        limit_comments=3,
        sort=sort,
        passkey=passkey,
        apiversion=api_version,
        displaycode=displaycode,
    )
    resp = session.get(BAZAARVOICE_ENDPOINT, params=params, timeout=45)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("HasErrors"):
        raise ReviewDownloaderError(f"BV error: {payload.get('Errors')}")
    return payload


def fetch_bv_simple_page(
    session: requests.Session,
    *,
    product_id: str,
    passkey: str,
    api_version: str,
    page_size: int,
    offset: int,
    sort: str,
    content_locale: str = "en*",
    locale: str = "en_US",
    include: str = "Products,Comments",
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "apiversion": api_version,
        "passkey": passkey,
        "Include": include,
        "Stats": "Reviews",
        "Limit": int(page_size),
        "Offset": int(offset),
        "Sort": sort,
        "Filter": [f"ProductId:{product_id}"],
    }
    if content_locale:
        params["Filter"].insert(0, f"contentlocale:{content_locale}")
    if locale:
        params["Locale"] = locale
    resp = session.get(BAZAARVOICE_ENDPOINT, params=params, timeout=45)
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("HasErrors"):
        raise ReviewDownloaderError(f"BV error: {payload.get('Errors')}")
    return payload


def fetch_bazaarvoice_raw_page(session: requests.Session, *, api_url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    resp = session.get(api_url, params=params, timeout=45)
    resp.raise_for_status()
    payload = resp.json()
    if isinstance(payload, dict) and payload.get("HasErrors"):
        raise ReviewDownloaderError(f"BV error: {payload.get('Errors')}")
    return payload


def fetch_powerreviews_page(
    session: requests.Session,
    *,
    merchant_id: str,
    locale: str,
    product_id: str,
    apikey: str,
    paging_from: int = 0,
    page_size: int = POWERREVIEWS_MAX_PAGE_SIZE,
    sort: str = "Newest",
    filters: str = "",
    search: str = "",
    image_only: bool = False,
    page_locale: Optional[str] = None,
) -> Dict[str, Any]:
    endpoint = POWERREVIEWS_ENDPOINT_TEMPLATE.format(
        merchant_id=merchant_id,
        locale=locale,
        product_id=product_id,
    )
    safe_page_size = max(1, min(int(page_size or POWERREVIEWS_MAX_PAGE_SIZE), POWERREVIEWS_MAX_PAGE_SIZE))
    params = {
        "paging.from": int(paging_from),
        "paging.size": safe_page_size,
        "filters": filters or "",
        "search": search or "",
        "sort": sort or "Newest",
        "image_only": "true" if image_only else "false",
        "page_locale": page_locale or locale,
        "_noconfig": "true",
        "apikey": apikey,
    }
    resp = session.get(endpoint, params=params, timeout=45)
    resp.raise_for_status()
    payload = resp.json()
    if isinstance(payload, dict) and payload.get("errors"):
        raise ReviewDownloaderError(f"PowerReviews error: {payload.get('errors')}")
    return payload


def powerreviews_extract_reviews(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("reviews"), list):
        return [review for review in payload["reviews"] if isinstance(review, dict)]
    results = payload.get("results") or []
    if isinstance(results, list):
        all_reviews: List[Dict[str, Any]] = []
        for result in results:
            if not isinstance(result, dict):
                continue
            nested = result.get("reviews")
            if isinstance(nested, list):
                all_reviews.extend([review for review in nested if isinstance(review, dict)])
            elif result.get("review_id"):
                all_reviews.append(result)
        return all_reviews
    return []


def extract_powerreviews_embeds(html_text: str) -> List[Dict[str, str]]:
    text = (html_text or "")
    if not text:
        return []
    normalized = (
        text.replace(r"\/", "/")
        .replace("&amp;", "&")
        .replace(r"\u0026", "&")
        .replace(r"\x26", "&")
    )
    found: List[Dict[str, str]] = []
    pattern = re.compile(
        r"(?:https?:)?//display\.powerreviews\.com/m/(\d+)/l/([A-Za-z_]+)/product/([^/?\"'>&]+)/reviews([^\"'>]*)",
        flags=re.IGNORECASE,
    )
    for match in pattern.finditer(normalized):
        merchant_id, locale, product_id, tail = match.groups()
        api_key_match = re.search(r"[?&]apikey=([^&#\"']+)", tail or "", flags=re.IGNORECASE)
        page_locale_match = re.search(r"[?&]page_locale=([^&#\"']+)", tail or "", flags=re.IGNORECASE)
        if merchant_id and locale and product_id and api_key_match:
            found.append(
                {
                    "merchant_id": merchant_id,
                    "locale": locale,
                    "product_id": product_id,
                    "apikey": api_key_match.group(1),
                    "page_locale": page_locale_match.group(1) if page_locale_match else locale,
                }
            )
    deduped: List[Dict[str, str]] = []
    seen = set()
    for item in found:
        key = (
            safe_text(item.get("merchant_id")).lower(),
            safe_text(item.get("locale")).lower(),
            safe_text(item.get("product_id")).lower(),
            safe_text(item.get("apikey")).lower(),
            safe_text(item.get("page_locale")).lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def extract_ulta_powerreviews_candidates(product_url: str, product_html: str = "") -> List[str]:
    candidates: List[str] = []
    for source in [product_url or "", product_html or ""]:
        candidates.extend(re.findall(r"\b(xlsImpprod\d{5,20})\b", source, flags=re.IGNORECASE))
        candidates.extend(re.findall(r"\b(pimprod\d{5,20})\b", source, flags=re.IGNORECASE))
    candidates.extend(
        re.findall(
            r"display\.powerreviews\.com/m/\d+/l/[A-Za-z_]+/product/([^/?\"'&<>]+)/reviews",
            product_html or "",
            flags=re.IGNORECASE,
        )
    )
    parsed = urlparse(product_url or "")
    query = parse_qs(parsed.query)
    candidates.extend(query.get("sku", []))
    return dedupe_keep_order(candidates)



def load_bazaarvoice_api_url(api_url: str, *, product_url_hint: str = "", retailer_hint: str = "", progress_ui=None) -> Dict[str, Any]:
    session = get_session()
    parsed = urlparse(api_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    params = parse_qs(parsed.query, keep_blank_values=True)
    page_size = int(_query_first_ci(params, ["limit", "Limit"], default=DEFAULT_PAGE_SIZE) or DEFAULT_PAGE_SIZE)
    product_id = _extract_bazaarvoice_product_id_from_params(params) or "UNKNOWN_PRODUCT"

    _report_progress(progress_ui, progress=0.05, title="Review feed detected", detail="Loading Bazaarvoice reviews from the matched API endpoint.")
    first = fetch_bazaarvoice_raw_page(session, api_url=base_url, params=params)
    total = int(first.get("TotalResults", 0) or 0)
    raw_reviews = list(first.get("Results") or [])
    total_requests = max(1, math.ceil(total / max(page_size, 1))) if total else 1
    _report_progress(
        progress_ui,
        progress=0.22,
        title="Fetching reviews",
        detail=f"Matched Bazaarvoice product {product_id}. {total:,} review(s) reported across about {total_requests} request(s).",
    )

    if total > len(raw_reviews):
        offsets = list(range(len(raw_reviews), total, page_size))
        for index, offset in enumerate(offsets, start=2):
            query = _clone_params(params)
            _set_ci_param(query, ["offset", "Offset"], int(offset))
            _set_ci_param(query, ["limit", "Limit"], int(page_size))
            payload = fetch_bazaarvoice_raw_page(session, api_url=base_url, params=query)
            raw_reviews.extend(payload.get("Results") or [])
            fraction = index / max(total_requests, 1)
            _report_progress(
                progress_ui,
                progress=min(0.22 + (0.68 * fraction), 0.9),
                title="Fetching reviews",
                detail=f"Loaded Bazaarvoice page {index} of {total_requests}.",
            )

    source_label = product_url_hint or api_url
    _report_progress(progress_ui, progress=0.95, title="Normalizing reviews", detail=f"{len(raw_reviews):,} review(s) collected from Bazaarvoice.")
    return build_bv_dataset(
        raw_reviews,
        product_url=product_url_hint or api_url,
        product_id=product_id,
        total=total,
        page_size=page_size,
        requests_needed=total_requests,
        source_label=source_label,
        retailer=retailer_hint,
        source_system="Bazaarvoice API",
    )


def load_powerreviews_api_url(api_url: str, *, product_url_hint: str = "", retailer_hint: str = "", progress_ui=None) -> Dict[str, Any]:
    session = get_session()
    parsed = urlparse(api_url)
    match = re.search(r"/m/(\d+)/l/([A-Za-z_]+)/product/([^/]+)/reviews", parsed.path)
    if not match:
        raise ReviewDownloaderError("Could not parse PowerReviews API URL.")
    merchant_id, locale, product_id = match.groups()
    params = parse_qs(parsed.query, keep_blank_values=True)
    apikey = _query_first_ci(params, ["apikey"])
    if not apikey:
        raise ReviewDownloaderError("PowerReviews API URL missing apikey.")
    sort = _query_first_ci(params, ["sort"], default="Newest") or "Newest"
    page_locale = _query_first_ci(params, ["page_locale"], default=locale) or locale
    requested_size = int(_query_first_ci(params, ["paging.size"], default=POWERREVIEWS_MAX_PAGE_SIZE) or POWERREVIEWS_MAX_PAGE_SIZE)
    page_size = min(requested_size, POWERREVIEWS_MAX_PAGE_SIZE)

    _report_progress(progress_ui, progress=0.05, title="Review feed detected", detail="Loading PowerReviews reviews from the matched API endpoint.")
    first = fetch_powerreviews_page(
        session,
        merchant_id=merchant_id,
        locale=locale,
        product_id=product_id,
        apikey=apikey,
        paging_from=0,
        page_size=page_size,
        sort=sort,
        page_locale=page_locale,
    )
    paging = first.get("paging") or {}
    total = int(paging.get("total_results", 0) or 0)
    server_page_size = int(paging.get("size", page_size) or page_size)
    results = first.get("results") or []
    product_name = safe_text((results[0].get("rollup") or {}).get("name") if results and isinstance(results[0], dict) else "")
    all_reviews = powerreviews_extract_reviews(first)
    total_requests = max(1, math.ceil(total / max(server_page_size, 1))) if total else 1
    _report_progress(
        progress_ui,
        progress=0.22,
        title="Fetching reviews",
        detail=f"Matched PowerReviews product {product_id}. {total:,} review(s) reported across about {total_requests} request(s).",
    )

    for page_index, start in enumerate(range(server_page_size, total, server_page_size), start=2):
        payload = fetch_powerreviews_page(
            session,
            merchant_id=merchant_id,
            locale=locale,
            product_id=product_id,
            apikey=apikey,
            paging_from=start,
            page_size=server_page_size,
            sort=sort,
            page_locale=page_locale,
        )
        all_reviews.extend(powerreviews_extract_reviews(payload))
        fraction = page_index / max(total_requests, 1)
        _report_progress(
            progress_ui,
            progress=min(0.22 + (0.68 * fraction), 0.9),
            title="Fetching reviews",
            detail=f"Loaded PowerReviews page {page_index} of {total_requests}.",
        )

    source_label = product_url_hint or api_url
    _report_progress(progress_ui, progress=0.95, title="Normalizing reviews", detail=f"{len(all_reviews):,} review(s) collected from PowerReviews.")
    return build_powerreviews_dataset(
        all_reviews,
        product_url=product_url_hint or api_url,
        product_id=product_id,
        total=total,
        page_size=server_page_size,
        requests_needed=total_requests,
        source_label=source_label,
        product_name=product_name,
        retailer=retailer_hint,
        source_system="PowerReviews API",
    )

def fetch_okendo_raw_page(session: requests.Session, *, api_url: str) -> Dict[str, Any]:
    resp = session.get(api_url, timeout=45, headers={"okendo-api-version": OKENDO_API_VERSION, "Accept": "application/json"})
    resp.raise_for_status()
    payload = resp.json()
    if isinstance(payload, dict) and payload.get("error"):
        raise ReviewDownloaderError(f"Okendo error: {payload.get('error')}")
    return payload if isinstance(payload, dict) else {}


def _resolve_okendo_next_url(current_url: str, next_url: str) -> str:
    current = urlparse(current_url)
    if re.match(r"^https?://", safe_text(next_url), flags=re.IGNORECASE):
        return next_url
    if str(next_url).startswith("/stores/"):
        return f"{current.scheme}://{current.netloc}/v1{next_url}"
    if str(next_url).startswith("/v1/"):
        return f"{current.scheme}://{current.netloc}{next_url}"
    if str(next_url).startswith("/"):
        return f"{current.scheme}://{current.netloc}{next_url}"
    return urljoin(current_url, next_url)


def _extract_okendo_product_id_from_api_url(api_url: str) -> str:
    parsed = urlparse(api_url)
    match = re.search(r"/products/([^/]+)/reviews$", parsed.path)
    return safe_text(match.group(1)) if match else "UNKNOWN_PRODUCT"


def _normalize_protocol_relative_url(url: str, *, default_scheme: str = "https") -> str:
    value = safe_text(url)
    if value.startswith("//"):
        return f"{default_scheme}:{value}"
    return value


def _currentbody_product_json_url(product_url: str) -> tuple[str, str]:
    parsed = urlparse(product_url)
    match = re.search(r"/products/([^/?#]+)", parsed.path or "", flags=re.IGNORECASE)
    if not match:
        raise ReviewDownloaderError("CurrentBody link must include a /products/{handle} path.")
    handle = safe_text(match.group(1)).strip().strip("/")
    return f"{parsed.scheme}://{parsed.netloc}/products/{handle}.js", handle


def build_currentbody_okendo_api_url(session: requests.Session, product_url: str, *, cfg: Dict[str, Any], product_html: str = "") -> str:
    json_url, handle = _currentbody_product_json_url(product_url)
    numeric_product_id = ""
    try:
        resp = session.get(json_url, timeout=35, headers={"Accept": "application/json"})
        resp.raise_for_status()
        payload = resp.json() if hasattr(resp, "json") else {}
        numeric_product_id = safe_text((payload or {}).get("id"))
    except Exception:
        numeric_product_id = ""

    if not numeric_product_id and product_html:
        patterns = [
            r'"productId"\s*:\s*"shopify-(\d+)"',
            r'"product_id"\s*:\s*"shopify-(\d+)"',
            r'"product"\s*:\s*\{[^{}]*"id"\s*:\s*(\d+)',
            r'"id"\s*:\s*(\d{8,})',
        ]
        for pattern in patterns:
            match = re.search(pattern, product_html or "", flags=re.IGNORECASE | re.DOTALL)
            if match:
                numeric_product_id = safe_text(match.group(1))
                if numeric_product_id:
                    break

    if not numeric_product_id:
        raise ReviewDownloaderError(f"Could not determine the Shopify product ID for CurrentBody handle '{handle}'.")

    params = {
        "limit": max(1, min(int(cfg.get("page_size", OKENDO_MAX_PAGE_SIZE) or OKENDO_MAX_PAGE_SIZE), OKENDO_MAX_PAGE_SIZE)),
        "orderBy": safe_text(cfg.get("order_by", "date desc")) or "date desc",
        "locale": safe_text(cfg.get("locale", "en")) or "en",
    }
    return f"{OKENDO_API_ROOT}/stores/{cfg['okendo_user_id']}/products/shopify-{numeric_product_id}/reviews?{urlencode(params)}"



def load_okendo_api_url(api_url: str, *, product_url_hint: str = "", retailer_hint: str = "", progress_ui=None) -> Dict[str, Any]:
    session = get_session()
    current_url = api_url
    _report_progress(progress_ui, progress=0.05, title="Review feed detected", detail="Loading Okendo reviews from the matched API endpoint.")
    first = fetch_okendo_raw_page(session, api_url=current_url)
    all_reviews = list(first.get("reviews") or [])
    requests_needed = 1
    next_url = safe_text(first.get("nextUrl"))
    _report_progress(progress_ui, progress=0.24, title="Fetching reviews", detail=f"Loaded Okendo page 1. {len(all_reviews):,} review(s) collected so far.")

    while next_url:
        current_url = _resolve_okendo_next_url(current_url, next_url)
        payload = fetch_okendo_raw_page(session, api_url=current_url)
        all_reviews.extend(payload.get("reviews") or [])
        next_url = safe_text(payload.get("nextUrl"))
        requests_needed += 1
        _report_progress(
            progress_ui,
            progress=min(0.24 + (0.13 * requests_needed), 0.9),
            title="Fetching reviews",
            detail=f"Loaded Okendo page {requests_needed}. {len(all_reviews):,} review(s) collected so far.",
        )

    product_id = _extract_okendo_product_id_from_api_url(api_url)
    primary_review = next((review for review in all_reviews if isinstance(review, dict)), {})
    product_name = safe_text(primary_review.get("productName"))
    inferred_product_url = _normalize_protocol_relative_url(safe_text(primary_review.get("productUrl")))
    source_label = product_url_hint or api_url
    _report_progress(progress_ui, progress=0.95, title="Normalizing reviews", detail=f"{len(all_reviews):,} review(s) collected from Okendo.")
    return build_okendo_dataset(
        all_reviews,
        product_url=product_url_hint or inferred_product_url or api_url,
        product_id=product_id,
        total=len(all_reviews),
        page_size=max(1, min(int(_query_first_ci(parse_qs(urlparse(api_url).query), ["limit"], default=OKENDO_MAX_PAGE_SIZE) or OKENDO_MAX_PAGE_SIZE), OKENDO_MAX_PAGE_SIZE)),
        requests_needed=requests_needed,
        source_label=source_label,
        product_name=product_name,
        retailer=retailer_hint,
        source_system="Okendo API",
    )


def _load_currentbody_product_page(session: requests.Session, *, product_url: str, product_html: str, cfg: Dict[str, Any], progress_ui=None) -> Dict[str, Any]:
    _report_progress(progress_ui, progress=0.05, title="Scanning product page", detail="Looking for embedded Okendo review feeds.")
    embedded_urls = [url for url in extract_embedded_review_api_urls(product_html) if is_okendo_api_url(url)]
    for api_index, api_url in enumerate(embedded_urls, start=1):
        try:
            _report_progress(
                progress_ui,
                progress=0.14,
                title="Embedded review feed found",
                detail=f"Trying embedded Okendo feed {api_index} of {len(embedded_urls)}.",
            )
            return load_okendo_api_url(
                api_url,
                product_url_hint=product_url,
                retailer_hint=cfg.get("retailer", "CurrentBody"),
                progress_ui=_scale_progress(progress_ui, start=0.18, end=0.95, prefix="Okendo API"),
            )
        except Exception:
            continue

    _report_progress(progress_ui, progress=0.18, title="Resolving review feed", detail="Matching the CurrentBody product page with its Okendo review endpoint.")
    api_url = build_currentbody_okendo_api_url(session, product_url, cfg=cfg, product_html=product_html)
    return load_okendo_api_url(
        api_url,
        product_url_hint=product_url,
        retailer_hint=cfg.get("retailer", "CurrentBody"),
        progress_ui=_scale_progress(progress_ui, start=0.22, end=0.95, prefix="Okendo API"),
    )


def _probe_bazaarvoice_candidates(session: requests.Session, *, candidates: Sequence[str], cfg: Dict[str, Any], progress_ui=None):
    tried: List[str] = []
    zero_match: Optional[Tuple[str, Dict[str, Any]]] = None
    candidate_list = dedupe_keep_order(candidates)
    total_candidates = len(candidate_list)
    if total_candidates:
        _report_progress(progress_ui, progress=0.05, title="Matching review feed", detail=f"Trying {total_candidates} Bazaarvoice product ID candidate(s).")
    for index, candidate in enumerate(candidate_list, start=1):
        _report_progress(
            progress_ui,
            progress=min(0.05 + (0.8 * ((index - 1) / max(total_candidates, 1))), 0.88),
            title="Matching review feed",
            detail=f"Trying candidate {index} of {total_candidates}: {candidate}",
        )
        try:
            if cfg.get("kind") == "action":
                payload = fetch_reviews_page(
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
                payload = fetch_bv_simple_page(
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
                _report_progress(progress_ui, progress=1.0, title="Review feed matched", detail=f"Matched Bazaarvoice candidate {candidate}.")
                return candidate, payload
            if zero_match is None:
                zero_match = (candidate, payload)
        except Exception as exc:
            tried.append(f"{candidate}: {exc}")
    if zero_match is not None:
        return zero_match
    raise ReviewDownloaderError("Could not match a Bazaarvoice product ID. Tried: " + "; ".join(tried[:8]))


def _probe_powerreviews_candidates(session: requests.Session, *, candidates: Sequence[str], cfg: Dict[str, Any], progress_ui=None):
    tried: List[str] = []
    zero_match: Optional[Tuple[str, Dict[str, Any]]] = None
    candidate_list = dedupe_keep_order(candidates)
    total_candidates = len(candidate_list)
    if total_candidates:
        _report_progress(progress_ui, progress=0.05, title="Matching review feed", detail=f"Trying {total_candidates} PowerReviews product ID candidate(s).")
    for index, candidate in enumerate(candidate_list, start=1):
        _report_progress(
            progress_ui,
            progress=min(0.05 + (0.8 * ((index - 1) / max(total_candidates, 1))), 0.88),
            title="Matching review feed",
            detail=f"Trying candidate {index} of {total_candidates}: {candidate}",
        )
        try:
            payload = fetch_powerreviews_page(
                session,
                merchant_id=cfg["merchant_id"],
                locale=cfg.get("locale", "en_US"),
                product_id=candidate,
                apikey=cfg["apikey"],
                paging_from=0,
                page_size=5,
                sort=cfg.get("sort", "Newest"),
                page_locale=cfg.get("page_locale", cfg.get("locale", "en_US")),
            )
            paging = payload.get("paging") or {}
            total = int(paging.get("total_results", 0) or 0)
            results = payload.get("results") or []
            extracted = powerreviews_extract_reviews(payload)
            if total > 0 or results or extracted:
                _report_progress(progress_ui, progress=1.0, title="Review feed matched", detail=f"Matched PowerReviews candidate {candidate}.")
                return candidate, payload
            if zero_match is None:
                zero_match = (candidate, payload)
        except Exception as exc:
            tried.append(f"{candidate}: {exc}")
    if zero_match is not None:
        return zero_match
    raise ReviewDownloaderError("Could not match a PowerReviews product ID. Tried: " + "; ".join(tried[:8]))


def _fetch_all_bazaarvoice_for_candidate(session: requests.Session, *, product_url: str, product_id: str, cfg: Dict[str, Any], progress_ui=None) -> Dict[str, Any]:
    _report_progress(progress_ui, progress=0.05, title="Fetching reviews", detail=f"Matched Bazaarvoice product {product_id}. Loading review pages.")
    if cfg.get("kind") == "action":
        first = fetch_reviews_page(
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
        total_requests = max(1, math.ceil(total / DEFAULT_PAGE_SIZE)) if total else 1
        _report_progress(progress_ui, progress=0.22, title="Fetching reviews", detail=f"Loaded Bazaarvoice page 1 of {total_requests}. {total:,} review(s) reported.")
        for page_index, offset in enumerate(range(len(raw_reviews), total, DEFAULT_PAGE_SIZE), start=2):
            page = fetch_reviews_page(
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
            fraction = page_index / max(total_requests, 1)
            _report_progress(progress_ui, progress=min(0.22 + (0.68 * fraction), 0.9), title="Fetching reviews", detail=f"Loaded Bazaarvoice page {page_index} of {total_requests}.")
    else:
        first = fetch_bv_simple_page(
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
        total_requests = max(1, math.ceil(total / DEFAULT_PAGE_SIZE)) if total else 1
        _report_progress(progress_ui, progress=0.22, title="Fetching reviews", detail=f"Loaded Bazaarvoice page 1 of {total_requests}. {total:,} review(s) reported.")
        for page_index, offset in enumerate(range(len(raw_reviews), total, DEFAULT_PAGE_SIZE), start=2):
            page = fetch_bv_simple_page(
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
            fraction = page_index / max(total_requests, 1)
            _report_progress(progress_ui, progress=min(0.22 + (0.68 * fraction), 0.9), title="Fetching reviews", detail=f"Loaded Bazaarvoice page {page_index} of {total_requests}.")
    _report_progress(progress_ui, progress=0.95, title="Normalizing reviews", detail=f"{len(raw_reviews):,} review(s) collected from Bazaarvoice.")
    return build_bv_dataset(
        raw_reviews,
        product_url=product_url,
        product_id=product_id,
        total=total,
        page_size=DEFAULT_PAGE_SIZE,
        requests_needed=total_requests,
        source_label=product_url,
        retailer=cfg.get("retailer", ""),
        source_system=cfg.get("source_system", "Bazaarvoice"),
    )


def _fetch_all_powerreviews_for_candidate(session: requests.Session, *, product_url: str, product_id: str, cfg: Dict[str, Any], progress_ui=None) -> Dict[str, Any]:
    _report_progress(progress_ui, progress=0.05, title="Fetching reviews", detail=f"Matched PowerReviews product {product_id}. Loading review pages.")
    first = fetch_powerreviews_page(
        session,
        merchant_id=cfg["merchant_id"],
        locale=cfg.get("locale", "en_US"),
        product_id=product_id,
        apikey=cfg["apikey"],
        paging_from=0,
        page_size=POWERREVIEWS_MAX_PAGE_SIZE,
        sort=cfg.get("sort", "Newest"),
        page_locale=cfg.get("page_locale", cfg.get("locale", "en_US")),
    )
    paging = first.get("paging") or {}
    total = int(paging.get("total_results", 0) or 0)
    server_page_size = int(paging.get("size", POWERREVIEWS_MAX_PAGE_SIZE) or POWERREVIEWS_MAX_PAGE_SIZE)
    results = first.get("results") or []
    product_name = safe_text((results[0].get("rollup") or {}).get("name") if results and isinstance(results[0], dict) else "")
    all_reviews = powerreviews_extract_reviews(first)
    total_requests = max(1, math.ceil(total / max(server_page_size, 1))) if total else 1
    _report_progress(progress_ui, progress=0.22, title="Fetching reviews", detail=f"Loaded PowerReviews page 1 of {total_requests}. {total:,} review(s) reported.")
    for page_index, start in enumerate(range(server_page_size, total, server_page_size), start=2):
        payload = fetch_powerreviews_page(
            session,
            merchant_id=cfg["merchant_id"],
            locale=cfg.get("locale", "en_US"),
            product_id=product_id,
            apikey=cfg["apikey"],
            paging_from=start,
            page_size=server_page_size,
            sort=cfg.get("sort", "Newest"),
            page_locale=cfg.get("page_locale", cfg.get("locale", "en_US")),
        )
        all_reviews.extend(powerreviews_extract_reviews(payload))
        fraction = page_index / max(total_requests, 1)
        _report_progress(progress_ui, progress=min(0.22 + (0.68 * fraction), 0.9), title="Fetching reviews", detail=f"Loaded PowerReviews page {page_index} of {total_requests}.")
    _report_progress(progress_ui, progress=0.95, title="Normalizing reviews", detail=f"{len(all_reviews):,} review(s) collected from PowerReviews.")
    return build_powerreviews_dataset(
        all_reviews,
        product_url=product_url,
        product_id=product_id,
        total=total,
        page_size=server_page_size,
        requests_needed=total_requests,
        source_label=product_url,
        product_name=product_name,
        retailer=cfg.get("retailer", ""),
        source_system=cfg.get("source_system", "PowerReviews"),
    )


def _load_bazaarvoice_product_page(session: requests.Session, *, product_url: str, product_html: str, cfg: Dict[str, Any], extra_candidates: Optional[Sequence[str]] = None, progress_ui=None) -> Dict[str, Any]:
    _report_progress(progress_ui, progress=0.05, title="Scanning product page", detail="Looking for embedded Bazaarvoice review feeds.")
    embedded_urls = [url for url in extract_embedded_review_api_urls(product_html) if is_bazaarvoice_api_url(url)]
    for api_index, api_url in enumerate(embedded_urls, start=1):
        try:
            _report_progress(
                progress_ui,
                progress=0.14,
                title="Embedded review feed found",
                detail=f"Trying embedded Bazaarvoice feed {api_index} of {len(embedded_urls)}.",
            )
            return load_bazaarvoice_api_url(
                api_url,
                product_url_hint=product_url,
                retailer_hint=cfg.get("retailer", ""),
                progress_ui=_scale_progress(progress_ui, start=0.18, end=0.95, prefix="Bazaarvoice API"),
            )
        except Exception:
            continue

    candidates: List[str] = []
    candidates.extend(extra_candidates or [])
    candidates.extend(extract_candidate_tokens_from_url(product_url))
    candidates.extend(extract_candidate_tokens_from_html(product_html))
    fallback_pid = _extract_generic_bv_product_id(product_url, product_html)
    if fallback_pid:
        candidates.insert(0, fallback_pid)
    candidate_list = dedupe_keep_order(candidates)
    _report_progress(progress_ui, progress=0.18, title="Matching review feed", detail=f"Trying {len(candidate_list)} Bazaarvoice product ID candidate(s).")
    product_id, _ = _probe_bazaarvoice_candidates(
        session,
        candidates=candidate_list,
        cfg=cfg,
        progress_ui=_scale_progress(progress_ui, start=0.22, end=0.48, prefix="Bazaarvoice match"),
    )
    return _fetch_all_bazaarvoice_for_candidate(
        session,
        product_url=product_url,
        product_id=product_id,
        cfg=cfg,
        progress_ui=_scale_progress(progress_ui, start=0.5, end=0.95, prefix="Bazaarvoice fetch"),
    )


def _load_powerreviews_product_page(session: requests.Session, *, product_url: str, product_html: str, cfg: Dict[str, Any], extra_candidates: Optional[Sequence[str]] = None, progress_ui=None) -> Dict[str, Any]:
    _report_progress(progress_ui, progress=0.05, title="Scanning product page", detail="Looking for embedded PowerReviews review feeds.")
    embedded_urls = [url for url in extract_embedded_review_api_urls(product_html) if is_powerreviews_api_url(url)]
    for api_index, api_url in enumerate(embedded_urls, start=1):
        try:
            _report_progress(
                progress_ui,
                progress=0.14,
                title="Embedded review feed found",
                detail=f"Trying embedded PowerReviews feed {api_index} of {len(embedded_urls)}.",
            )
            return load_powerreviews_api_url(
                api_url,
                product_url_hint=product_url,
                retailer_hint=cfg.get("retailer", ""),
                progress_ui=_scale_progress(progress_ui, start=0.18, end=0.95, prefix="PowerReviews API"),
            )
        except Exception:
            continue

    embeds = extract_powerreviews_embeds(product_html)
    for embed_index, embed in enumerate(embeds, start=1):
        try:
            cfg2 = dict(cfg)
            cfg2.update({key: value for key, value in embed.items() if value})
            _report_progress(
                progress_ui,
                progress=0.18,
                title="Embedded review feed found",
                detail=f"Trying embedded PowerReviews configuration {embed_index} of {len(embeds)}.",
            )
            return _fetch_all_powerreviews_for_candidate(
                session,
                product_url=product_url,
                product_id=cfg2["product_id"],
                cfg=cfg2,
                progress_ui=_scale_progress(progress_ui, start=0.24, end=0.95, prefix="PowerReviews fetch"),
            )
        except Exception:
            continue

    candidates: List[str] = []
    candidates.extend(extra_candidates or [])
    host = (urlparse(product_url).netloc or "").lower()
    if "ulta.com" in host:
        candidates.extend(extract_ulta_powerreviews_candidates(product_url, product_html))
    candidates.extend(extract_candidate_tokens_from_html(product_html))
    candidates.extend(extract_candidate_tokens_from_url(product_url))
    candidate_list = dedupe_keep_order(candidates)
    _report_progress(progress_ui, progress=0.2, title="Matching review feed", detail=f"Trying {len(candidate_list)} PowerReviews product ID candidate(s).")
    product_id, _ = _probe_powerreviews_candidates(
        session,
        candidates=candidate_list,
        cfg=cfg,
        progress_ui=_scale_progress(progress_ui, start=0.24, end=0.5, prefix="PowerReviews match"),
    )
    return _fetch_all_powerreviews_for_candidate(
        session,
        product_url=product_url,
        product_id=product_id,
        cfg=cfg,
        progress_ui=_scale_progress(progress_ui, start=0.52, end=0.95, prefix="PowerReviews fetch"),
    )


def load_product_reviews(product_url: str, *, progress_ui=None) -> Dict[str, Any]:
    def _finish(dataset: Dict[str, Any]) -> Dict[str, Any]:
        reviews_df = dataset.get("reviews_df")
        review_count = len(reviews_df) if isinstance(reviews_df, pd.DataFrame) else 0
        label = safe_text(dataset.get("source_label")) or host or product_url
        _report_progress(progress_ui, progress=1.0, title="Reviews loaded", detail=f"{review_count:,} review(s) ready from {label}.")
        return dataset

    product_url = normalize_input_url(product_url)
    parsed = urlparse(product_url)
    host = strip_www(parsed.netloc)
    retailer_hint = ""
    if "costco.com" in host:
        retailer_hint = "Costco"
    elif "sephora.com" in host:
        retailer_hint = "Sephora"
    elif "ulta.com" in host:
        retailer_hint = "Ulta"
    elif "hoka.com" in host:
        retailer_hint = "Hoka"
    elif "currentbody.com" in host:
        retailer_hint = "CurrentBody"
    elif looks_like_sharkninja_uk_eu(host):
        retailer_hint = "SharkNinja UK/EU"
    elif looks_like_sharkninja_us(host):
        retailer_hint = "SharkNinja"

    _report_progress(progress_ui, progress=0.02, title="Preparing source", detail=f"Checking {host or 'the source link'} for a supported review feed.")

    if is_bazaarvoice_api_url(product_url):
        return _finish(load_bazaarvoice_api_url(product_url, progress_ui=_scale_progress(progress_ui, start=0.08, end=0.96, prefix="Bazaarvoice")))
    if is_powerreviews_api_url(product_url):
        return _finish(load_powerreviews_api_url(product_url, progress_ui=_scale_progress(progress_ui, start=0.08, end=0.96, prefix="PowerReviews")))
    if is_okendo_api_url(product_url):
        return _finish(load_okendo_api_url(product_url, retailer_hint=retailer_hint, progress_ui=_scale_progress(progress_ui, start=0.08, end=0.96, prefix="Okendo")))

    session = get_session()
    product_html = ""
    page_fetch_error = None
    try:
        _report_progress(progress_ui, progress=0.08, title="Loading product page", detail=f"Fetching {host or product_url} and scanning it for embedded review feeds.")
        resp = session.get(product_url, timeout=35)
        resp.raise_for_status()
        product_html = resp.text or ""
        _report_progress(progress_ui, progress=0.22, title="Product page loaded", detail="Scanning the page for embedded Bazaarvoice, PowerReviews, or Okendo endpoints.")
    except Exception as exc:
        page_fetch_error = exc
        product_html = ""
        _report_progress(progress_ui, progress=0.22, title="Product page unavailable", detail=f"Falling back to known review patterns for {host or product_url}.")

    embedded_urls = extract_embedded_review_api_urls(product_html) if product_html else []
    if embedded_urls:
        _report_progress(progress_ui, progress=0.28, title="Embedded review feed found", detail=f"Found {len(embedded_urls)} embedded review endpoint(s) on the page.")
        for api_index, api_url in enumerate(embedded_urls, start=1):
            try:
                child = _scale_progress(progress_ui, start=0.32, end=0.96, prefix=f"Embedded feed {api_index} of {len(embedded_urls)}")
                if is_bazaarvoice_api_url(api_url):
                    return _finish(load_bazaarvoice_api_url(api_url, product_url_hint=product_url, retailer_hint=retailer_hint, progress_ui=child))
                if is_powerreviews_api_url(api_url):
                    return _finish(load_powerreviews_api_url(api_url, product_url_hint=product_url, retailer_hint=retailer_hint, progress_ui=child))
                if is_okendo_api_url(api_url):
                    return _finish(load_okendo_api_url(api_url, product_url_hint=product_url, retailer_hint=retailer_hint, progress_ui=child))
            except Exception:
                continue

    if "costco.com" in host:
        return _finish(
            _load_bazaarvoice_product_page(
                session,
                product_url=product_url,
                product_html=product_html,
                cfg={
                    "kind": "action",
                    "passkey": next(cfg["passkey"] for cfg in SITE_REVIEW_CONFIGS if cfg["key"] == "costco"),
                    "displaycode": "2070_2_0-en_us",
                    "api_version": "5.5",
                    "sort": "SubmissionTime:desc",
                    "content_locales": "en_US,ar*,zh*,hr*,cs*,da*,nl*,en*,et*,fi*,fr*,de*,el*,he*,hu*,id*,it*,ja*,ko*,lv*,lt*,ms*,no*,pl*,pt*,ro*,sk*,sl*,es*,sv*,th*,tr*,vi*",
                    "retailer": "Costco",
                    "source_system": "Bazaarvoice",
                },
                progress_ui=_scale_progress(progress_ui, start=0.28, end=0.96, prefix="Costco feed"),
            )
        )

    if "sephora.com" in host:
        sephora_candidates: List[str] = []
        sephora_candidates.extend(re.findall(r"(P\d{5,10})", product_url, flags=re.IGNORECASE))
        sephora_candidates.extend(re.findall(r"(P\d{5,10})", product_html or "", flags=re.IGNORECASE))
        return _finish(
            _load_bazaarvoice_product_page(
                session,
                product_url=product_url,
                product_html=product_html,
                cfg={
                    "kind": "simple",
                    "passkey": next(cfg["passkey"] for cfg in SITE_REVIEW_CONFIGS if cfg["key"] == "sephora"),
                    "api_version": "5.4",
                    "sort": "SubmissionTime:desc",
                    "content_locale": "en*",
                    "locale": "en_US",
                    "source_system": "Bazaarvoice",
                    "retailer": "Sephora",
                },
                extra_candidates=sephora_candidates,
                progress_ui=_scale_progress(progress_ui, start=0.28, end=0.96, prefix="Sephora feed"),
            )
        )

    if "ulta.com" in host:
        return _finish(
            _load_powerreviews_product_page(
                session,
                product_url=product_url,
                product_html=product_html,
                cfg={
                    "merchant_id": "6406",
                    "locale": "en_US",
                    "page_locale": "en_US",
                    "apikey": next(cfg["api_key"] for cfg in SITE_REVIEW_CONFIGS if cfg["key"] == "ulta"),
                    "sort": "Newest",
                    "retailer": "Ulta",
                    "source_system": "PowerReviews",
                },
                extra_candidates=extract_ulta_powerreviews_candidates(product_url, product_html),
                progress_ui=_scale_progress(progress_ui, start=0.28, end=0.96, prefix="Ulta feed"),
            )
        )

    if "currentbody.com" in host:
        return _finish(
            _load_currentbody_product_page(
                session,
                product_url=product_url,
                product_html=product_html,
                cfg=next((dict(cfg) for cfg in SITE_REVIEW_CONFIGS if cfg["key"] == "currentbody"), {
                    "okendo_user_id": "",
                    "locale": "en",
                    "order_by": "date desc",
                    "page_size": OKENDO_MAX_PAGE_SIZE,
                    "retailer": "CurrentBody",
                }),
                progress_ui=_scale_progress(progress_ui, start=0.28, end=0.96, prefix="CurrentBody feed"),
            )
        )

    if "hoka.com" in host:
        hoka_candidates: List[str] = []
        hoka_candidates.extend(re.findall(r"/(\d+)\.html", parsed.path))
        hoka_candidates.extend(re.findall(r"dwvar_(\d+)_", product_url))
        hoka_candidates.extend(re.findall(r"Item\s*No\.?\s*(\d{5,10})", product_html or "", flags=re.IGNORECASE))
        hoka_candidates.extend(re.findall(r'"product_page_id"\s*[:=]\s*"?(\d{5,10})', product_html or "", flags=re.IGNORECASE))
        hoka_candidates.extend(re.findall(r'"page_id"\s*[:=]\s*"?(\d{5,10})', product_html or "", flags=re.IGNORECASE))
        return _finish(
            _load_powerreviews_product_page(
                session,
                product_url=product_url,
                product_html=product_html,
                cfg={
                    "merchant_id": "437772",
                    "locale": "en_US",
                    "page_locale": "en_US",
                    "apikey": next(cfg["api_key"] for cfg in SITE_REVIEW_CONFIGS if cfg["key"] == "hoka"),
                    "sort": "Newest",
                    "retailer": "Hoka",
                    "source_system": "PowerReviews",
                },
                extra_candidates=hoka_candidates,
                progress_ui=_scale_progress(progress_ui, start=0.28, end=0.96, prefix="Hoka feed"),
            )
        )

    if looks_like_sharkninja_uk_eu(host):
        uk_pid = _extract_generic_bv_product_id(product_url, product_html)
        return _finish(
            _load_bazaarvoice_product_page(
                session,
                product_url=product_url,
                product_html=product_html,
                cfg={
                    "kind": "simple",
                    "passkey": next(cfg["passkey"] for cfg in SITE_REVIEW_CONFIGS if cfg["key"] == "sharkninja_uk_eu"),
                    "api_version": "5.4",
                    "locale": "en_GB",
                    "include": "Products,Comments",
                    "sort": "SubmissionTime:desc",
                    "content_locale": "en*",
                    "retailer": "SharkNinja UK/EU",
                    "source_system": "Bazaarvoice",
                },
                extra_candidates=[uk_pid] if uk_pid else None,
                progress_ui=_scale_progress(progress_ui, start=0.28, end=0.96, prefix="SharkNinja UK/EU feed"),
            )
        )

    pid = _extract_generic_bv_product_id(product_url, product_html)
    if pid:
        try:
            return _finish(
                _fetch_all_bazaarvoice_for_candidate(
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
                    progress_ui=_scale_progress(progress_ui, start=0.32, end=0.96, prefix="Matched feed"),
                )
            )
        except Exception:
            pass

    generic_candidates: List[str] = []
    generic_candidates.extend(extract_candidate_tokens_from_url(product_url))
    generic_candidates.extend(extract_candidate_tokens_from_html(product_html))
    try:
        return _finish(
            _load_bazaarvoice_product_page(
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
                progress_ui=_scale_progress(progress_ui, start=0.32, end=0.96, prefix="Generic feed"),
            )
        )
    except Exception:
        if page_fetch_error is not None:
            if isinstance(page_fetch_error, requests.HTTPError):
                raise
            raise ReviewDownloaderError(f"Could not load product page or match a review feed: {page_fetch_error}")
        raise


def load_multiple_product_reviews(urls: Sequence[str] | str, *, progress_ui=None) -> Dict[str, Any]:
    url_list = parse_bulk_product_urls(urls if isinstance(urls, str) else "\n".join([str(url) for url in (urls or [])]))
    if not url_list:
        raise ReviewDownloaderError("Add at least one product or review URL.")
    if len(url_list) == 1:
        return load_product_reviews(url_list[0], progress_ui=progress_ui)

    frames: List[pd.DataFrame] = []
    loaded: List[Dict[str, Any]] = []
    failures: List[Tuple[str, str]] = []
    total_urls = len(url_list)
    _report_progress(progress_ui, progress=0.03, title="Preparing combined workspace", detail=f"{total_urls} source link(s) queued for scraping.")

    for index, url in enumerate(url_list, start=1):
        host = strip_www(urlparse(url).netloc) or url
        start = 0.08 + (0.76 * ((index - 1) / max(total_urls, 1)))
        end = 0.08 + (0.76 * (index / max(total_urls, 1)))
        _report_progress(progress_ui, progress=start, title="Loading source", detail=f"Starting source {index} of {total_urls}: {host}")
        try:
            dataset = load_product_reviews(url, progress_ui=_scale_progress(progress_ui, start=start, end=end, prefix=f"Source {index} of {total_urls}"))
            frame = dataset["reviews_df"].copy()
            frame["loaded_from_url"] = url
            frame["loaded_from_host"] = strip_www(urlparse(url).netloc) or "Unknown"
            frame["loaded_from_label"] = safe_text(dataset.get("source_label")) or url
            frame["loaded_from_batch"] = f"Link {index}"
            frames.append(frame)
            loaded.append(dataset)
            _report_progress(progress_ui, progress=end, title="Source loaded", detail=f"Loaded source {index} of {total_urls}: {len(frame):,} review(s) from {host}.")
        except Exception as exc:
            failures.append((url, str(exc)))
            _report_progress(progress_ui, progress=end, title="Source skipped", detail=f"Could not load source {index} of {total_urls}: {host}. {exc}")

    if not frames:
        details = "; ".join(f"{url} -> {err}" for url, err in failures[:3]) if failures else "No links loaded."
        raise ReviewDownloaderError(f"Could not load any links. {details}")

    _report_progress(progress_ui, progress=0.9, title="Combining reviews", detail=f"Loaded {len(loaded)} of {total_urls} source link(s). Merging and deduplicating reviews.")
    combined = pd.concat(frames, ignore_index=True)
    if "review_id" in combined.columns:
        exact_key = (
            combined["review_id"].fillna("").astype(str).str.strip()
            + "||" + combined.get("product_id", pd.Series("", index=combined.index)).fillna("").astype(str).str.strip()
            + "||" + combined.get("review_text", pd.Series("", index=combined.index)).fillna("").astype(str).str.strip()
        )
        combined = combined.loc[~exact_key.duplicated(keep="first")].copy()
        counts = Counter()
        unique_ids: List[str] = []
        for rid in combined["review_id"].fillna("").astype(str).str.strip().tolist():
            base = rid or f"review_{len(unique_ids) + 1}"
            counts[base] += 1
            unique_ids.append(base if counts[base] == 1 else f"{base} ({counts[base]})")
        combined["review_id"] = unique_ids
    combined = finalize_df(combined)

    summary = ReviewBatchSummary(
        product_url="\n".join(url_list),
        product_id=f"MULTI_URL_WORKSPACE_{len(url_list)}",
        total_reviews=len(combined),
        page_size=max(len(combined), 1),
        requests_needed=sum(int(getattr(dataset.get("summary"), "requests_needed", 0)) for dataset in loaded),
        reviews_downloaded=len(combined),
    )
    _report_progress(progress_ui, progress=1.0, title="Combined workspace ready", detail=f"{len(combined):,} review(s) ready across {len(loaded)} loaded source link(s).")
    return {
        "summary": summary,
        "reviews_df": combined,
        "source_type": "multi-url",
        "source_label": format_multi_source_label(url_list),
        "source_urls": url_list,
        "source_failures": failures,
    }

def load_uploaded_files(files, *, include_local_symptomization: bool = False) -> Dict[str, Any]:
    if not files:
        raise ReviewDownloaderError("Upload at least one file.")
    frames = [read_uploaded_file(file, include_local_symptomization=include_local_symptomization) for file in files]
    combined = pd.concat(frames, ignore_index=True)
    combined["review_id"] = combined["review_id"].astype(str)
    combined = combined.drop_duplicates(subset=["review_id"], keep="first").reset_index(drop=True)
    combined = finalize_df(combined)
    product_id = (
        next((value for value in combined["base_sku"].fillna("").astype(str) if value.strip()), "")
        or next((value for value in combined["product_id"].fillna("").astype(str) if value.strip()), "")
        or "UPLOADED_REVIEWS"
    )
    names = [getattr(file, "name", "file") for file in files]
    source = names[0] if len(names) == 1 else f"{len(names)} uploaded files"
    summary = ReviewBatchSummary(
        product_url="",
        product_id=product_id,
        total_reviews=len(combined),
        page_size=max(len(combined), 1),
        requests_needed=0,
        reviews_downloaded=len(combined),
    )
    source_sheet_name = frames[0].attrs.get("source_sheet_name") if len(frames) == 1 else ""
    return {
        "summary": summary,
        "reviews_df": combined,
        "source_type": "uploaded",
        "source_label": source,
        "source_sheet_name": source_sheet_name,
    }
