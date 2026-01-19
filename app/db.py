import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from app.settings import get_settings


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(settings.db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS documents(
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                source_type TEXT,
                published_date TEXT NULL,
                content_text TEXT,
                content_hash TEXT,
                image_url TEXT NULL,
                created_at TEXT,
                updated_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chunks(
                id INTEGER PRIMARY KEY,
                document_id INTEGER,
                chunk_index INTEGER,
                heading TEXT NULL,
                chunk_text TEXT,
                UNIQUE(document_id, chunk_index)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_events(
                id INTEGER PRIMARY KEY,
                created_at TEXT,
                client_city TEXT NULL,
                client_region TEXT NULL,
                client_country TEXT NULL,
                user_agent TEXT,
                message TEXT,
                response_preview TEXT,
                model TEXT,
                retrieved_count INTEGER,
                latency_ms INTEGER,
                input_tokens INTEGER NULL,
                output_tokens INTEGER NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ingest_runs(
                id INTEGER PRIMARY KEY,
                created_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                status TEXT,
                stats_json TEXT,
                error TEXT,
                pages_crawled INTEGER DEFAULT 0,
                documents_seen INTEGER DEFAULT 0,
                chunks_embedded INTEGER DEFAULT 0
            )
            """
        )
        _ensure_column(conn, "documents", "image_url", "TEXT")
        _ensure_column(conn, "ingest_runs", "pages_crawled", "INTEGER")
        _ensure_column(conn, "ingest_runs", "documents_seen", "INTEGER")
        _ensure_column(conn, "ingest_runs", "chunks_embedded", "INTEGER")
        _ensure_column(conn, "chat_events", "input_tokens", "INTEGER")
        _ensure_column(conn, "chat_events", "output_tokens", "INTEGER")


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    existing = {row[1] for row in rows}
    if column in existing:
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


@contextmanager
def get_conn():
    settings = get_settings()
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def insert_chat_event(
    *,
    client_city: str | None,
    client_region: str | None,
    client_country: str | None,
    user_agent: str,
    message: str,
    response_preview: str,
    model: str,
    retrieved_count: int,
    latency_ms: int,
    input_tokens: int | None,
    output_tokens: int | None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO chat_events(
                created_at,
                client_city,
                client_region,
                client_country,
                user_agent,
                message,
                response_preview,
                model,
                retrieved_count,
                latency_ms,
                input_tokens,
                output_tokens
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _utcnow(),
                client_city,
                client_region,
                client_country,
                user_agent,
                message,
                response_preview,
                model,
                retrieved_count,
                latency_ms,
                input_tokens,
                output_tokens,
            ),
        )


def recent_queries(limit: int = 10) -> list[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT created_at, message
            FROM chat_events
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()


def create_ingest_run() -> int:
    with get_conn() as conn:
        cursor = conn.execute(
            """
            INSERT INTO ingest_runs(created_at, started_at, status, pages_crawled, documents_seen, chunks_embedded)
            VALUES (?, ?, ?, 0, 0, 0)
            """,
            (_utcnow(), _utcnow(), "running"),
        )
        return int(cursor.lastrowid)


def finish_ingest_run(run_id: int, status: str, stats_json: str | None, error: str | None) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE ingest_runs
            SET finished_at = ?, status = ?, stats_json = ?, error = ?
            WHERE id = ?
            """,
            (_utcnow(), status, stats_json, error, run_id),
        )


def update_ingest_progress(run_id: int, *, pages_crawled: int, documents_seen: int, chunks_embedded: int) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE ingest_runs
            SET pages_crawled = ?, documents_seen = ?, chunks_embedded = ?
            WHERE id = ?
            """,
            (pages_crawled, documents_seen, chunks_embedded, run_id),
        )


def latest_ingest_run() -> sqlite3.Row | None:
    with get_conn() as conn:
        return conn.execute(
            """
            SELECT id, created_at, started_at, finished_at, status, stats_json, error,
                   pages_crawled, documents_seen, chunks_embedded
            FROM ingest_runs
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
