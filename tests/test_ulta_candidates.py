from review_analyst.connectors import extract_ulta_powerreviews_candidates


def test_extract_ulta_candidates_prefers_powerreviews_page_id():
    url = "https://www.ulta.com/p/porcelain-ceramic-straightening-iron-xlsImpprod13891067?sku=2302209"
    html = '"product_page_id":"xlsImpprod13891067"'
    candidates = extract_ulta_powerreviews_candidates(url, html)
    assert candidates[0].lower() == "xlsimpprod13891067"
    assert "2302209" in candidates
