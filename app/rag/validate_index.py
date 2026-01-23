from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate FAISS index vs SQLite DB.")
    parser.add_argument("--db", default="app.db", help="Path to SQLite DB")
    parser.add_argument("--index", default="data/faiss.index", help="Path to FAISS index")
    return parser.parse_args()


def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def main() -> int:
    args = parse_args()
    db_path = Path(args.db).expanduser()
    index_path = Path(args.index).expanduser()

    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")
    if not index_path.exists():
        raise SystemExit(f"FAISS index not found: {index_path}")

    with open_db(db_path) as conn:
        excluded_exists = conn.execute(
            "SELECT name FROM pragma_table_info('documents') WHERE name = 'excluded'"
        ).fetchone()
        excluded_clause = "AND (documents.excluded IS NULL OR documents.excluded = 0)" if excluded_exists else ""
        chunk_count = conn.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM chunks
            JOIN documents ON chunks.document_id = documents.id
            WHERE 1=1 {excluded_clause}
            """
        ).fetchone()["count"]

        distinct_docs = conn.execute(
            f"""
            SELECT COUNT(DISTINCT documents.id) AS count
            FROM documents
            JOIN chunks ON chunks.document_id = documents.id
            WHERE 1=1 {excluded_clause}
            """
        ).fetchone()["count"]

    import faiss

    index = faiss.read_index(str(index_path))
    index_count = index.ntotal

    print(f"DB chunks (eligible): {chunk_count}")
    print(f"DB docs with chunks: {distinct_docs}")
    print(f"FAISS vectors: {index_count}")
    if chunk_count == index_count:
        print("✅ FAISS count matches DB chunk count.")
    else:
        print("⚠️ FAISS count does not match DB chunk count.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
