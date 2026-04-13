from __future__ import annotations

import os

import pytest

from review_analyst.models import ReviewDownloaderError
from review_analyst.normalization import read_uploaded_file
from review_analyst.utils import normalize_input_url, parse_bulk_product_urls


def test_normalize_input_url_blocks_localhost_and_private_ip():
    with pytest.raises(ReviewDownloaderError):
        normalize_input_url("http://localhost:8501/test")
    with pytest.raises(ReviewDownloaderError):
        normalize_input_url("http://127.0.0.1/test")
    with pytest.raises(ReviewDownloaderError):
        normalize_input_url("http://10.0.0.8/test")


def test_normalize_input_url_accepts_public_https_url():
    assert normalize_input_url("www.costco.com/item") == "https://www.costco.com/item"


def test_parse_bulk_product_urls_keeps_public_links_and_dedupes():
    urls = parse_bulk_product_urls("""www.costco.com/item
https://www.costco.com/item
https://www.sephora.com/product""")
    assert urls == [
        "https://www.costco.com/item",
        "https://www.sephora.com/product",
    ]


class _UploadedFile:
    def __init__(self, name: str, payload: bytes):
        self.name = name
        self._payload = payload

    def getvalue(self) -> bytes:
        return self._payload


def test_read_uploaded_file_blocks_large_payload(monkeypatch):
    monkeypatch.setenv("STARWALK_MAX_UPLOAD_MB", "1")
    payload = b"a" * (1024 * 1024 + 1)
    uploaded = _UploadedFile("reviews.csv", payload)
    with pytest.raises(ValueError, match="upload limit"):
        read_uploaded_file(uploaded)
