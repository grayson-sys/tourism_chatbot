from __future__ import annotations

import argparse
import hashlib
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

JUNK_PATTERNS = [
    "/tag/",
    "/tags/",
    "/category/",
    "/author/",
    "/page/",
    "page=",
    "/search",
    "?s=",
    "/feed",
    "/rss",
    "/wp-json",
    "/wp-admin",
]


def normalize_url(url: str) -> str:
    parsed = urlsplit(url)
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    cleaned = parsed._replace(fragment="", path=path)
    return cleaned.geturl()


def normalized_text_hash(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text.strip().lower())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def url_bucket(url: str) -> str:
    parsed = urlsplit(url)
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return f"{parsed.netloc}/"
    return f"{parsed.netloc}/{parts[0]}"


def open_db(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report ingest quality stats.")
    parser.add_argument("--db", default="app.db", help="Path to SQLite DB")
    parser.add_argument("--min-short", type=int, default=500)
    parser.add_argument("--min-medium", type=int, default=1500)
    parser.add_argument("--limit", type=int, default=50)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    with open_db(db_path) as conn:
        total_docs = conn.execute("SELECT COUNT(*) AS count FROM documents").fetchone()["count"]
        total_chunks = conn.execute("SELECT COUNT(*) AS count FROM chunks").fetchone()["count"]

        print(f"Report generated at {datetime.utcnow().isoformat()}Z")
        print(f"Total documents: {total_docs}")
        print(f"Total chunks: {total_chunks}")
        print()

        rows = conn.execute("SELECT id, url, content_text, content_hash FROM documents").fetchall()

    domain_counts = Counter()
    bucket_counts = Counter()
    short_docs = []
    medium_docs = []
    junk_counts = Counter()
    content_hash_counts = Counter()
    normalized_hash_counts = Counter()
    normalized_hash_map = defaultdict(list)

    for row in rows:
        url = row["url"]
        text = row["content_text"] or ""
        domain = urlsplit(url).netloc
        domain_counts[domain] += 1
        bucket_counts[url_bucket(url)] += 1

        for pattern in JUNK_PATTERNS:
            if pattern in url:
                junk_counts[pattern] += 1
                break

        length = len(text)
        if length < args.min_short:
            short_docs.append((length, url))
        elif length < args.min_medium:
            medium_docs.append((length, url))

        content_hash = row["content_hash"] or ""
        if content_hash:
            content_hash_counts[content_hash] += 1

        if text:
            n_hash = normalized_text_hash(text)
            normalized_hash_counts[n_hash] += 1
            normalized_hash_map[n_hash].append(url)

    print("Counts by domain:")
    for domain, count in domain_counts.most_common():
        print(f"- {domain}: {count}")
    print()

    print("Top URL buckets by volume:")
    for bucket, count in bucket_counts.most_common(20):
        print(f"- {bucket}: {count}")
    print()

    print("Junk pattern hits:")
    for pattern, count in junk_counts.most_common():
        print(f"- {pattern}: {count}")
    print()

    duplicates_by_hash = [(h, c) for h, c in content_hash_counts.items() if c > 1]
    print(f"Duplicate content_hash count: {len(duplicates_by_hash)}")
    if duplicates_by_hash:
        for h, c in sorted(duplicates_by_hash, key=lambda item: item[1], reverse=True)[: args.limit]:
            print(f"- {h}: {c}")
    print()

    near_dupes = [(h, c) for h, c in normalized_hash_counts.items() if c > 1]
    print(f"Near-duplicate normalized hash count: {len(near_dupes)}")
    if near_dupes:
        for h, c in sorted(near_dupes, key=lambda item: item[1], reverse=True)[: args.limit]:
            sample = ", ".join(normalized_hash_map[h][:3])
            print(f"- {h}: {c} urls=[{sample}]")
    print()

    print(f"Short documents (<{args.min_short} chars): {len(short_docs)}")
    for length, url in sorted(short_docs)[: args.limit]:
        print(f"- {length} {url}")
    print()

    print(f"Medium documents (<{args.min_medium} chars): {len(medium_docs)}")
    for length, url in sorted(medium_docs)[: args.limit]:
        print(f"- {length} {url}")
    print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
