from app.rag.crawl import url_allowed


def test_allowlist_empty_allows():
    assert url_allowed("https://example.com/page", [], [])


def test_denylist_blocks():
    assert not url_allowed("https://example.com/blocked", [], ["blocked"])


def test_allowlist_required():
    assert url_allowed("https://example.com/good", ["good"], [])
    assert not url_allowed("https://example.com/bad", ["good"], [])
