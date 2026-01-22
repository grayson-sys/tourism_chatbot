from app.rag.crawl import denylist_reason, normalize_url


def test_normalize_url_strips_fragment_and_tracking():
    url = "https://example.com/path/?utm_source=google&keep=1#section"
    assert normalize_url(url) == "https://example.com/path?keep=1"


def test_denylist_reason_matches_substring():
    denylist = ["bad", "skip-this"]
    assert denylist_reason("https://example.com/skip-this/page", denylist) == "skip-this"
    assert denylist_reason("https://example.com/good", denylist) is None
