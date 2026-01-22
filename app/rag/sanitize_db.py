from __future__ import annotations

import argparse
import hashlib
import re
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit, urlunsplit


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


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalized_text_hash(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text.strip().lower())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {row[1] for row in rows}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sanitize ingest database.")
    parser.add_argument("--db", default="app.db", help="Path to SQLite DB")
    parser.add_argument("--apply", action="store_true", help="Apply changes (default is dry-run)")
    parser.add_argument("--min-chars", type=int, default=500)
    parser.add_argument("--min-words", type=int, default=80)
    parser.add_argument("--limit", type=int, default=50)
    return parser.parse_args()


def pick_canonical(rows: list[sqlite3.Row]) -> sqlite3.Row:
    def score(row: sqlite3.Row) -> tuple[int, str]:
        length = row["text_length"] or 0
        updated = row["updated_at"] or ""
        return (length, updated)

    return sorted(rows, key=score, reverse=True)[0]


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    with open_db(db_path) as conn:
        ensure_column(conn, "documents", "excluded", "INTEGER DEFAULT 0")
        ensure_column(conn, "documents", "excluded_reason", "TEXT")
        ensure_column(conn, "documents", "normalized_url", "TEXT")
        ensure_column(conn, "documents", "canonical_url", "TEXT")
        ensure_column(conn, "documents", "text_length", "INTEGER")
        ensure_column(conn, "documents", "normalized_hash", "TEXT")

        rows = conn.execute(
            """
            SELECT id, url, content_text, content_hash, normalized_hash, excluded, updated_at
            FROM documents
            """
        ).fetchall()

        updates = []
        junk_updates = []
        computed_rows = []
        for row in rows:
            text = row["content_text"] or ""
            words = len(text.split())
            length = len(text)
            normalized_url = normalize_url(row["url"])
            content_hash = row["content_hash"] or hash_text(text)
            normalized_hash = row["normalized_hash"] or normalized_text_hash(text)
            canonical_url = normalized_url

            reason = None
            if not text.strip():
                reason = "empty_text"
            elif length < args.min_chars or words < args.min_words:
                reason = f"short_text:{length}:{words}"
            else:
                for pattern in JUNK_PATTERNS:
                    if pattern in row["url"]:
                        reason = f"junk:{pattern}"
                        break

            updates.append(
                (
                    normalized_url,
                    canonical_url,
                    length,
                    content_hash,
                    normalized_hash,
                    row["id"],
                )
            )
            computed_rows.append(
                {
                    "id": row["id"],
                    "normalized_url": normalized_url,
                    "normalized_hash": normalized_hash,
                    "text_length": length,
                    "updated_at": row["updated_at"] or "",
                    "excluded": row["excluded"] or 0,
                }
            )
            if reason:
                junk_updates.append((reason, row["id"]))

        if args.apply:
            conn.executemany(
                """
                UPDATE documents
                SET normalized_url = ?, canonical_url = ?, text_length = ?, content_hash = ?, normalized_hash = ?
                WHERE id = ?
                """,
                updates,
            )
            conn.executemany(
                """
                UPDATE documents
                SET excluded = 1, excluded_reason = ?
                WHERE id = ? AND (excluded IS NULL OR excluded = 0)
                """,
                junk_updates,
            )

        # Dedupe by normalized_url
        by_url: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in computed_rows:
            by_url[row["normalized_url"]].append(row)

        dedupe_updates = []
        for url, group in by_url.items():
            if len(group) <= 1:
                continue
            canonical = pick_canonical(group)
            for row in group:
                if row["id"] == canonical["id"]:
                    continue
                if row["excluded"]:
                    continue
                dedupe_updates.append((f"duplicate_url:{canonical['id']}", row["id"]))

        if args.apply and dedupe_updates:
            conn.executemany(
                """
                UPDATE documents
                SET excluded = 1, excluded_reason = ?
                WHERE id = ? AND (excluded IS NULL OR excluded = 0)
                """,
                dedupe_updates,
            )

        # Dedupe by normalized_hash
        by_hash: dict[str, list[sqlite3.Row]] = defaultdict(list)
        for row in computed_rows:
            by_hash[row["normalized_hash"]].append(row)

        hash_updates = []
        for n_hash, group in by_hash.items():
            if len(group) <= 1:
                continue
            canonical = pick_canonical(group)
            for row in group:
                if row["id"] == canonical["id"]:
                    continue
                if row["excluded"]:
                    continue
                hash_updates.append((f"duplicate_text:{canonical['id']}", row["id"]))

        if args.apply and hash_updates:
            conn.executemany(
                """
                UPDATE documents
                SET excluded = 1, excluded_reason = ?
                WHERE id = ? AND (excluded IS NULL OR excluded = 0)
                """,
                hash_updates,
            )

        if args.apply:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_normalized_url ON documents(normalized_url)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_canonical_url ON documents(canonical_url)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_excluded ON documents(excluded)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_documents_normalized_hash ON documents(normalized_hash)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chunks_document_id ON chunks(document_id)")
            conn.commit()

        summary = Counter()
        summary["rows_total"] = len(computed_rows)
        summary["updates"] = len(updates)
        summary["junk_updates"] = len(junk_updates)
        summary["dedupe_url"] = len(dedupe_updates)
        summary["dedupe_hash"] = len(hash_updates)

        print("Sanitize summary")
        for key, value in summary.items():
            print(f"- {key}: {value}")
        if not args.apply:
            print("Dry-run only. Use --apply to write changes.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
