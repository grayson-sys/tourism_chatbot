from __future__ import annotations

import logging
import random
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from typing import Iterable
from urllib.parse import parse_qsl, urljoin, urlsplit, urlunsplit
from urllib.robotparser import RobotFileParser

import requests
import yaml
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_USER_AGENT = "NewMexicoConciergeBot/1.0 (+https://www.newmexico.org/)"
DEFAULT_TIMEOUT = (5, 15)
DEFAULT_MAX_REDIRECTS = 5
DEFAULT_RATE_LIMIT_SECONDS = 1.5
DEFAULT_RATE_LIMIT_JITTER = 0.5
DEFAULT_LOG_EVERY = 25

TRACKING_QUERY_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "mc_cid",
    "mc_eid",
}


@dataclass
class CrawlStats:
    pages_fetched: int = 0
    pages_skipped: int = 0
    errors_count: int = 0
    timeouts_count: int = 0
    robots_blocked_count: int = 0
    per_status_counts: Counter = field(default_factory=Counter)
    per_host_counts: Counter = field(default_factory=Counter)


def load_yaml_list(path) -> list[str]:
    if not path.exists():
        return []
    data = yaml.safe_load(path.read_text())
    if not data:
        return []
    if isinstance(data, list):
        return [str(item) for item in data]
    return []


def _get_logger(logger: logging.Logger | None) -> logging.Logger:
    if logger:
        return logger
    return logging.getLogger("crawler")


def _is_html_url(url: str) -> bool:
    path = urlsplit(url).path.lower()
    bad_exts = (".jpg", ".jpeg", ".png", ".gif", ".svg", ".pdf", ".zip", ".mp4", ".mp3")
    return not path.endswith(bad_exts)


def normalize_url(url: str) -> str:
    parsed = urlsplit(url)
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() not in TRACKING_QUERY_PARAMS
    ]
    query.sort()
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    cleaned = parsed._replace(fragment="", query="&".join(f"{k}={v}" for k, v in query), path=path)
    return urlunsplit(cleaned)


def _is_candidate_domain(url: str, allowed_hosts: set[str]) -> bool:
    if not allowed_hosts:
        return True
    return urlsplit(url).netloc in allowed_hosts


def _matches_allowlist(url: str, allowlist: Iterable[str]) -> bool:
    allowlist = [rule.strip() for rule in allowlist if rule.strip()]
    if not allowlist:
        return True
    url_lower = url.lower()
    return any(rule.lower() in url_lower for rule in allowlist)


def denylist_reason(url: str, denylist: Iterable[str]) -> str | None:
    url_lower = url.lower()
    for rule in denylist:
        rule_lower = str(rule).lower()
        if rule_lower and rule_lower in url_lower:
            return rule
    return None


def _get_robot_parser(url: str, cache: dict[str, RobotFileParser], logger: logging.Logger) -> RobotFileParser:
    parsed = urlsplit(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    if base in cache:
        return cache[base]
    rp = RobotFileParser()
    rp.set_url(urljoin(base, "/robots.txt"))
    try:
        rp.read()
    except Exception as exc:
        logger.warning("ROBOTS ERROR %s %s", base, exc)
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
    meta = soup.find("meta", {"name": "date"})
    if meta and meta.get("content"):
        return meta["content"]
    meta = soup.find("meta", {"name": "dc.date"})
    if meta and meta.get("content"):
        return meta["content"]
    meta = soup.find("meta", {"name": "dc.date.issued"})
    if meta and meta.get("content"):
        return meta["content"]
    time_tag = soup.find("time")
    if time_tag and time_tag.get("datetime"):
        return time_tag["datetime"]
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


def _build_session(max_redirects: int) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.max_redirects = max_redirects
    return session


def crawl(
    seeds: Iterable[str],
    allowlist: Iterable[str],
    denylist: Iterable[str],
    *,
    max_pages: int = 200,
    rate_limit_seconds: float = DEFAULT_RATE_LIMIT_SECONDS,
    rate_limit_jitter: float = DEFAULT_RATE_LIMIT_JITTER,
    max_redirects: int = DEFAULT_MAX_REDIRECTS,
    timeout: tuple[int, int] = DEFAULT_TIMEOUT,
    log_every: int = DEFAULT_LOG_EVERY,
    per_host_cap: int | None = None,
    stats: CrawlStats | None = None,
    logger: logging.Logger | None = None,
) -> Iterable[dict[str, str]]:
    logger = _get_logger(logger)
    stats = stats or CrawlStats()
    start_time = time.time()

    queue = deque(normalize_url(seed) for seed in seeds)
    visited: set[str] = set()
    last_request: dict[str, float] = {}
    rp_cache: dict[str, RobotFileParser] = {}

    allowed_hosts = {urlsplit(seed).netloc for seed in seeds}

    session = _build_session(max_redirects)
    session.headers.update({"User-Agent": DEFAULT_USER_AGENT})

    logger.info(
        "START %s max_pages=%s delay=%.2fs seeds=%s",
        time.strftime("%Y-%m-%d %H:%M:%S"),
        max_pages,
        rate_limit_seconds,
        list(seeds),
    )

    while queue and len(visited) < max_pages:
        raw_url = queue.popleft()
        url = normalize_url(raw_url)
        if url in visited:
            continue

        if not _is_candidate_domain(url, allowed_hosts):
            stats.pages_skipped += 1
            logger.info("DENY DOMAIN %s", url)
            continue

        deny_reason = denylist_reason(url, denylist)
        if deny_reason:
            stats.pages_skipped += 1
            logger.info("DENY LIST %s reason=%s", url, deny_reason)
            continue

        if not _matches_allowlist(url, allowlist):
            stats.pages_skipped += 1
            logger.info("DENY ALLOWLIST %s", url)
            continue

        if not _is_html_url(url):
            stats.pages_skipped += 1
            logger.info("DENY NON-HTML %s", url)
            continue

        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"}:
            stats.pages_skipped += 1
            logger.info("DENY SCHEME %s", url)
            continue

        if per_host_cap is not None and stats.per_host_counts[parsed.netloc] >= per_host_cap:
            stats.pages_skipped += 1
            logger.info("DENY HOST CAP %s", url)
            continue

        rp = _get_robot_parser(url, rp_cache, logger)
        if not rp.can_fetch(DEFAULT_USER_AGENT, url):
            stats.robots_blocked_count += 1
            stats.pages_skipped += 1
            logger.info("ROBOTS BLOCK %s", url)
            continue

        now = time.time()
        last = last_request.get(parsed.netloc, 0)
        wait = rate_limit_seconds + random.uniform(0, rate_limit_jitter)
        if now - last < wait:
            time.sleep(wait - (now - last))
        last_request[parsed.netloc] = time.time()

        logger.info("GET %s", url)
        start = time.time()
        try:
            resp = session.get(url, timeout=timeout, allow_redirects=True)
            elapsed_ms = int((time.time() - start) * 1000)
            final_url = normalize_url(resp.url)
            size_bytes = len(resp.content) if resp.content else 0
            logger.info(
                "GOT %s %s %s %s",
                resp.status_code,
                elapsed_ms,
                size_bytes,
                final_url,
            )
        except requests.exceptions.Timeout:
            elapsed_ms = int((time.time() - start) * 1000)
            stats.errors_count += 1
            stats.timeouts_count += 1
            logger.warning("ERR Timeout %s %s", elapsed_ms, url)
            continue
        except requests.exceptions.TooManyRedirects:
            elapsed_ms = int((time.time() - start) * 1000)
            stats.errors_count += 1
            logger.warning("ERR TooManyRedirects %s %s", elapsed_ms, url)
            continue
        except requests.exceptions.RequestException as exc:
            elapsed_ms = int((time.time() - start) * 1000)
            stats.errors_count += 1
            logger.warning("ERR %s %s %s", exc.__class__.__name__, elapsed_ms, url)
            continue

        visited.add(url)
        stats.pages_fetched += 1
        stats.per_status_counts[str(resp.status_code)] += 1
        stats.per_host_counts[parsed.netloc] += 1

        if resp.status_code >= 400:
            stats.pages_skipped += 1
            stats.errors_count += 1
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        title = _extract_title(soup)
        text = _extract_text(soup)
        published_date = _extract_date(soup)
        image_url = _extract_image(soup, url)

        elapsed = time.time() - start_time
        if stats.pages_fetched % max(1, log_every) == 0:
            rate = (stats.pages_fetched / elapsed) * 60 if elapsed else 0
            logger.info(
                "HEARTBEAT fetched=%s queue=%s elapsed=%.1fs rate=%.1f/min last=%s",
                stats.pages_fetched,
                len(queue),
                elapsed,
                rate,
                url,
            )

        if text:
            yield {
                "url": url,
                "title": title,
                "published_date": published_date,
                "content_text": text,
                "image_url": image_url,
                "_queue_size": str(len(queue)),
                "_elapsed": str(elapsed),
                "_last_url": url,
            }
        else:
            stats.pages_skipped += 1

        for link in _discover_links(url, soup):
            normalized = normalize_url(link)
            if normalized in visited:
                continue
            if not _is_html_url(normalized):
                continue
            if not _is_candidate_domain(normalized, allowed_hosts):
                continue
            if denylist_reason(normalized, denylist):
                continue
            if not _matches_allowlist(normalized, allowlist):
                continue
            queue.append(normalized)
