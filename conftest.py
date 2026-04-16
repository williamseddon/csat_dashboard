from __future__ import annotations

import html
import ipaddress
import json
import math
import re
from collections.abc import Sequence
from datetime import date, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import pandas as pd

from .config import STOPWORDS
from .models import ReviewDownloaderError


NON_VALUES = {"<NA>", "NA", "N/A", "NONE", "-", "", "NAN", "NULL"}


def safe_text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    if isinstance(value, (list, tuple, set, dict, pd.Series, pd.DataFrame, pd.Index)):
        return default
    try:
        missing = pd.isna(value)
    except Exception:
        missing = False
    if isinstance(missing, bool) and missing:
        return default
    text = str(value).strip()
    return default if text.lower() in {"nan", "none", "null", "<na>"} else text


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def safe_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = safe_text(value).lower()
    if text in {"true", "1", "yes", "y", "t"}:
        return True
    if text in {"false", "0", "no", "n", "f", ""}:
        return False
    return default


def safe_mean(series: pd.Series) -> Optional[float]:
    if series.empty:
        return None
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    return float(numeric.mean()) if not numeric.empty else None


def safe_pct(num: float, den: float) -> float:
    return 0.0 if not den else float(num) / float(den)


def fmt_num(value: Any, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "n/a"


def fmt_pct(value: Any, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{100 * float(value):.{digits}f}%"
    except Exception:
        return "n/a"


def trunc(text: Any, max_chars: int = 420) -> str:
    normalized = re.sub(r"\s+", " ", safe_text(text)).strip()
    return normalized if len(normalized) <= max_chars else normalized[: max_chars - 1].rstrip() + "…"


def norm_text(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text).lower()).strip()


def tokenize(text: Any) -> List[str]:
    return [token for token in re.findall(r"[a-z0-9']+", norm_text(text)) if len(token) > 2 and token not in STOPWORDS]


def estimate_tokens(text: Any) -> int:
    raw = str(text or "")
    if not raw:
        return 0
    return int(max(1, math.ceil(len(raw) / 4)))


def dedupe_keep_order(values: Sequence[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values or []:
        value = safe_text(raw).strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def ordered_unique(values: Sequence[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values or []:
        value = safe_text(raw).strip().strip("/")
        value = re.sub(r"\.html?$", "", value, flags=re.IGNORECASE)
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def normalize_input_url(product_url: str) -> str:
    product_url = (product_url or "").strip()
    if not product_url:
        return ""
    if not re.match(r"^https?://", product_url, flags=re.IGNORECASE):
        product_url = "https://" + product_url
    parsed = urlparse(product_url)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise ReviewDownloaderError("Only http(s) product/review URLs are supported.")
    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise ReviewDownloaderError("Enter a valid public product/review URL.")
    if host in {"localhost", "0.0.0.0"} or host.endswith(".localhost") or host.endswith(".local"):
        raise ReviewDownloaderError("Local or private URLs are blocked.")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None and (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast or ip.is_unspecified):
        raise ReviewDownloaderError("Local or private URLs are blocked.")
    return product_url


def strip_www(host: str) -> str:
    host = (host or "").lower().strip()
    return host[4:] if host.startswith("www.") else host


def host_matches(host: str, tokens: Sequence[str]) -> bool:
    normalized = strip_www(host)
    return any(token in normalized for token in tokens)


def domain_matches(host: str, domain: str) -> bool:
    host = safe_text(host).lower()
    domain = safe_text(domain).lower()
    return bool(host == domain or host.endswith("." + domain))


def is_bazaarvoice_api_url(url: str) -> bool:
    parsed = urlparse(safe_text(url))
    return "api.bazaarvoice.com" in parsed.netloc.lower() and parsed.path.lower().endswith("/reviews.json")


def is_powerreviews_api_url(url: str) -> bool:
    parsed = urlparse(safe_text(url))
    path = parsed.path.lower()
    return "display.powerreviews.com" in parsed.netloc.lower() and "/product/" in path and path.endswith("/reviews")


def is_okendo_api_url(url: str) -> bool:
    parsed = urlparse(safe_text(url))
    path = parsed.path.lower()
    return "api.okendo.io" in parsed.netloc.lower() and path.startswith("/v1/") and (path.endswith("/reviews") or path.endswith("/review_media") or path.endswith("/review_aggregate"))


def looks_like_sharkninja_uk_eu(host: str) -> bool:
    normalized = strip_www(host)
    if normalized in {
        "sharkninja.co.uk", "sharkninja.eu", "sharkninja.de", "sharkninja.fr", "sharkninja.es", "sharkninja.it", "sharkninja.nl", "sharkninja.ie",
        "sharkclean.co.uk", "ninjakitchen.co.uk", "sharkclean.eu", "ninjakitchen.eu",
        "sharkclean.de", "ninjakitchen.de", "sharkclean.fr", "ninjakitchen.fr",
        "sharkclean.nl", "ninjakitchen.nl", "sharkclean.ie", "ninjakitchen.ie",
    }:
        return True
    return ("sharkclean" in normalized or "ninjakitchen" in normalized or "sharkninja" in normalized) and not normalized.endswith(".com")


def looks_like_sharkninja_us(host: str) -> bool:
    normalized = strip_www(host)
    return normalized in {
        "sharkclean.com", "ninjakitchen.com", "sharkninja.com"
    } or normalized.endswith("sharkclean.com") or normalized.endswith("ninjakitchen.com")


def parse_bulk_product_urls(raw_text: str) -> List[str]:
    urls: List[str] = []
    seen = set()
    for line in re.split(r"[\r\n]+", str(raw_text or "")):
        candidate = re.sub(r"^[\s\-*\u2022\d\.)]+", "", str(line or "")).strip()
        if not candidate:
            continue
        normalized = normalize_input_url(candidate)
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        urls.append(normalized)
    return urls


def format_multi_source_label(urls: Sequence[str]) -> str:
    hosts: List[str] = []
    seen = set()
    for url in urls:
        host = strip_www(urlparse(url).netloc) or safe_text(url)
        if host and host not in seen:
            seen.add(host)
            hosts.append(host)
    if not hosts:
        return f"{len(urls)} links"
    if len(hosts) <= 3:
        return f"{len(urls)} links · " + ", ".join(hosts)
    return f"{len(urls)} links · " + ", ".join(hosts[:3]) + f" +{len(hosts) - 3}"


def extract_candidate_tokens_from_url(url: str) -> List[str]:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    candidates: List[str] = []

    for key, values in params.items():
        lower_key = key.lower()
        if lower_key in {"productid", "product_id", "itemid", "item_id", "pid", "id", "sku"}:
            candidates.extend(values)
        dw_match = re.match(r"dwvar_([^_]+)_", key, flags=re.IGNORECASE)
        if dw_match:
            candidates.append(dw_match.group(1))

    path = parsed.path or ""
    zid_match = re.search(r"[_\-]zid([A-Za-z0-9\-_]+)$", path, flags=re.IGNORECASE)
    if zid_match:
        candidates.append(zid_match.group(1))

    segments = [segment for segment in path.split("/") if segment]
    if segments:
        last = re.sub(r"\.html?$", "", segments[-1], flags=re.IGNORECASE)
        candidates.append(last)
        candidates.extend(re.findall(r"([A-Za-z0-9]{4,30})", last))
        if len(segments) >= 2:
            candidates.append(re.sub(r"\.html?$", "", segments[-2], flags=re.IGNORECASE))

    return dedupe_keep_order(candidates)


def extract_candidate_tokens_from_html(html_text: str) -> List[str]:
    if not html_text:
        return []
    text = html_text.replace(r"\/", "/")
    candidates: List[str] = []
    patterns = [
        r'Item\s*No\.?\s*([A-Za-z0-9_-]{4,40})',
        r'"productId"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'"product_id"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'"page_id"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'"product_page_id"\s*[:=]\s*"([A-Za-z0-9_-]{3,40})"',
        r'data-product-id=["\']([^"\']+)',
        r'data-sku=["\']([^"\']+)',
        r'display\.powerreviews\.com/m/\d+/l/[A-Za-z_]+/product/([^/"\'&?<>]+)/reviews',
        r'api\.bazaarvoice\.com/data/reviews\.json[^"\']*productid(?::eq)?:([^,&"\']+)',
        r'\b(P\d{5,10})\b',
        r'\b(pimprod\d{5,12})\b',
        r'\b(xlsImpprod\d{5,12})\b',
    ]
    for pattern in patterns:
        candidates.extend(re.findall(pattern, text, flags=re.IGNORECASE))
    return dedupe_keep_order(candidates)


def extract_embedded_review_api_urls(html_text: str) -> List[str]:
    text = (html_text or "")
    if not text:
        return []
    normalized = (
        text.replace(r"\/", "/")
        .replace("&amp;", "&")
        .replace(r"\u0026", "&")
        .replace(r"\x26", "&")
    )
    hits: List[str] = []
    patterns = [
        r"https://api\.bazaarvoice\.com/data/reviews\.json[^\"'<>\s]+",
        r"https://display\.powerreviews\.com/m/\d+/l/[A-Za-z_]+/product/[^\"'<>\s]+/reviews[^\"'<>\s]*",
        r"//display\.powerreviews\.com/m/\d+/l/[A-Za-z_]+/product/[^\"'<>\s]+/reviews[^\"'<>\s]*",
        r"/m/\d+/l/[A-Za-z_]+/product/[^\"'<>\s]+/reviews[^\"'<>\s]*apikey=[^\"'<>\s]+",
        r"https://api\.okendo\.io/v1/stores/[^\"'<>\s]+/reviews[^\"'<>\s]*",
    ]
    for pattern in patterns:
        for match in re.findall(pattern, normalized, flags=re.IGNORECASE):
            url = str(match).strip().strip('"\' ,)')
            if url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/m/"):
                url = "https://display.powerreviews.com" + url
            hits.append(url)
    return dedupe_keep_order(hits)


def format_filter_description(active_items: Sequence[tuple[str, str]]) -> str:
    return "; ".join(f"{key}={value}" for key, value in active_items) if active_items else "No active filters"


def esc(text: Any) -> str:
    return html.escape(str(text or ""))


def safe_json_load(raw: str) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        pass
    try:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start : end + 1])
    except Exception:
        pass
    return {}
