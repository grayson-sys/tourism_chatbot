from __future__ import annotations

import time
from collections import deque
from typing import Iterable
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
import yaml
from bs4 import BeautifulSoup

DEFAULT_USER_AGENT = "NewMexicoConciergeBot/1.0 (+https://www.newmexico.org/)"


def load_yaml_list(path) -> list[str]:
    if not path.exists():
        return []
    data = yaml.safe_load(path.read_text())
    if not data:
        return []
    if isinstance(data, list):
        return [str(item) for item in data]
    return []


def _is_html_url(url: str) -> bool:
    path = urlparse(url).path.lower()
    bad_exts = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".pdf", ".zip", ".mp4", ".mp3")
    return not path.endswith(bad_exts)


def _is_candidate_url(url: str) -> bool:
    url_lower = url.lower()
    if "newmexicomagazine.org" in url_lower:
        return True
    if "newmexico.org" in url_lower and "new-mexico-true-certified" in url_lower:
        return True
    return False


def _sitemap_urls(base_url: str) -> list[str]:
    sitemap_url = urljoin(base_url, "/sitemap.xml")
    try:
        resp = requests.get(sitemap_url, headers={"User-Agent": DEFAULT_USER_AGENT}, timeout=20)
        resp.raise_for_status()
    except Exception:
        return []
    soup = BeautifulSoup(resp.text, "xml")
    urls = []
    for loc in soup.find_all("loc"):
        if loc.text:
            urls.append(loc.text.strip())
    return urls


def url_allowed(url: str, allowlist: Iterable[str], denylist: Iterable[str]) -> bool:
    url_lower = url.lower()
    allowlist = [rule.lower() for rule in allowlist]
    denylist = [rule.lower() for rule in denylist]
    if any(rule in url_lower for rule in denylist):
        return False
    if not allowlist:
        return True
    return any(rule in url_lower for rule in allowlist)


def _get_robot_parser(url: str, cache: dict[str, RobotFileParser]) -> RobotFileParser:
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    if base in cache:
        return cache[base]
    rp = RobotFileParser()
    rp.set_url(urljoin(base, "/robots.txt"))
    try:
        rp.read()
    except Exception:
        rp.parse([])
    cache[base] = rp
    return rp


def _extract_text(soup: BeautifulSoup) -> str:
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    main = soup.find("main") or soup.find("article") or soup.body
    if not main:
        return soup.get_text(" ", strip=True)
    return " ".join(main.stripped_strings)


def _extract_title(soup: BeautifulSoup) -> str:
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    h1 = soup.find("h1")
    if h1:
        return h1.get_text(strip=True)
    return "Untitled"


def _extract_date(soup: BeautifulSoup) -> str | None:
    meta = soup.find("meta", {"property": "article:published_time"})
    if meta and meta.get("content"):
        return meta["content"]
    meta = soup.find("meta", {"name": "pubdate"})
    if meta and meta.get("content"):
        return meta["content"]
    return None


def _extract_image(soup: BeautifulSoup, base_url: str) -> str | None:
    og_image = soup.find("meta", {"property": "og:image"})
    if og_image and og_image.get("content"):
        return urljoin(base_url, og_image["content"])
    main = soup.find("main") or soup.find("article") or soup.body
    if not main:
        return None
    img = main.find("img")
    if img and img.get("src"):
        return urljoin(base_url, img["src"])
    return None


def _discover_links(base_url: str, soup: BeautifulSoup) -> list[str]:
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("#"):
            continue
        absolute = urljoin(base_url, href)
        links.append(absolute)
    return links


def crawl(
    seeds: Iterable[str],
    allowlist: Iterable[str],
    denylist: Iterable[str],
    max_pages: int = 200,
    rate_limit_seconds: float = 2.0,
) -> Iterable[dict[str, str]]:
    queue = deque(seeds)
    visited: set[str] = set()
    last_request: dict[str, float] = {}
    rp_cache: dict[str, RobotFileParser] = {}

    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_USER_AGENT})

    for seed in list(seeds):
        parsed = urlparse(seed)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for sitemap_url in _sitemap_urls(base):
            if url_allowed(sitemap_url, allowlist, denylist):
                queue.append(sitemap_url)

    while queue and len(visited) < max_pages:
        url = queue.popleft()
        if url in visited:
            continue
        if not url_allowed(url, allowlist, denylist):
            continue
        if not _is_candidate_url(url):
            continue
        if not _is_html_url(url):
            continue

        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            continue

        rp = _get_robot_parser(url, rp_cache)
        if not rp.can_fetch(DEFAULT_USER_AGENT, url):
            continue

        now = time.time()
        last = last_request.get(parsed.netloc, 0)
        if parsed.netloc.endswith("newmexico.org") and now - last < rate_limit_seconds:
            time.sleep(rate_limit_seconds - (now - last))
        last_request[parsed.netloc] = time.time()

        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
        except Exception:
            continue

        visited.add(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        title = _extract_title(soup)
        text = _extract_text(soup)
        published_date = _extract_date(soup)
        image_url = _extract_image(soup, url)

        if text:
            yield {
                "url": url,
                "title": title,
                "published_date": published_date,
                "content_text": text,
                "image_url": image_url,
            }

        for link in _discover_links(url, soup):
            if (
                link not in visited
                and url_allowed(link, allowlist, denylist)
                and _is_candidate_url(link)
                and _is_html_url(link)
            ):
                queue.append(link)
