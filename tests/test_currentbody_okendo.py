from review_analyst.connectors import build_currentbody_okendo_api_url, load_okendo_api_url


class FakeResponse:
    def __init__(self, payload=None, text="", status_code=200):
        self._payload = payload if payload is not None else {}
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if url not in self.payloads:
            raise AssertionError(f"Unexpected URL: {url}")
        return self.payloads[url]


def test_build_currentbody_okendo_api_url_uses_shopify_product_json():
    session = FakeSession(
        {
            "https://us.currentbody.com/products/currentbody-skin-neck-and-dec-perfector.js": FakeResponse(
                {"id": 4649274376289}
            )
        }
    )
    api_url = build_currentbody_okendo_api_url(
        session,
        "https://us.currentbody.com/products/currentbody-skin-neck-and-dec-perfector?variant=42060344000609",
        cfg={
            "okendo_user_id": "37e09e6e-c267-4f51-9ec1-d136d8570631",
            "page_size": 25,
            "order_by": "date desc",
            "locale": "en",
        },
    )
    assert api_url == (
        "https://api.okendo.io/v1/stores/37e09e6e-c267-4f51-9ec1-d136d8570631/"
        "products/shopify-4649274376289/reviews?limit=25&orderBy=date+desc&locale=en"
    )


def test_load_okendo_api_url_follows_next_url(monkeypatch):
    start_url = (
        "https://api.okendo.io/v1/stores/store-1/products/shopify-111/reviews"
        "?limit=25&orderBy=date+desc&locale=en"
    )
    next_url = (
        "https://api.okendo.io/v1/stores/store-1/products/shopify-111/reviews"
        "?lastEvaluated=cursor-1&limit=25&locale=en&orderBy=date%20desc"
    )

    review_one = {
        "reviewId": "r1",
        "productId": "shopify-111",
        "productName": "CurrentBody Test Product",
        "productUrl": "//us.currentbody.com/products/test-product",
        "title": "Great",
        "body": "Loved it",
        "rating": 5,
        "dateCreated": "2026-02-11T23:52:32.939Z",
        "languageCode": "en",
        "isRecommended": True,
        "helpfulCount": 2,
        "reviewer": {
            "displayName": "Reviewer A",
            "isVerified": True,
            "location": {"country": {"name": "United States", "code": "US"}},
            "attributes": [{"title": "Age Range", "type": "single-value", "value": "35 - 44"}],
        },
        "status": "approved",
    }
    review_two = {
        "reviewId": "r2",
        "productId": "shopify-111",
        "productName": "CurrentBody Test Product",
        "productUrl": "//us.currentbody.com/products/test-product",
        "title": "Solid",
        "body": "Works well",
        "rating": 4,
        "dateCreated": "2026-02-10T23:52:32.939Z",
        "languageCode": "en",
        "isRecommended": True,
        "reviewer": {
            "displayName": "Reviewer B",
            "location": {"country": {"name": "Australia", "code": "AU"}},
            "attributes": [],
        },
        "status": "approved",
        "isIncentivized": True,
    }

    session = FakeSession(
        {
            start_url: FakeResponse(
                {
                    "reviews": [review_one],
                    "nextUrl": "/stores/store-1/products/shopify-111/reviews?lastEvaluated=cursor-1&limit=25&locale=en&orderBy=date%20desc",
                }
            ),
            next_url: FakeResponse({"reviews": [review_two]}),
        }
    )
    monkeypatch.setattr("review_analyst.connectors.get_session", lambda: session)

    dataset = load_okendo_api_url(
        start_url,
        product_url_hint="https://us.currentbody.com/products/test-product",
        retailer_hint="CurrentBody",
    )

    assert dataset["source_type"] == "okendo"
    assert dataset["summary"].requests_needed == 2
    assert dataset["summary"].total_reviews == 2
    assert len(dataset["reviews_df"]) == 2
    assert set(dataset["reviews_df"]["product_id"].tolist()) == {"shopify-111"}
    assert dataset["reviews_df"].iloc[0]["retailer"] == "CurrentBody"
    assert set(dataset["reviews_df"]["user_location"].tolist()) == {"United States", "Australia"}
