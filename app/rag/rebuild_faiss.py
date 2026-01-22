from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path

import numpy as np
from openai import OpenAI

from app.rag.index_faiss import add_vectors, save_index
from app.settings import get_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild FAISS index from SQLite DB.")
    parser.add_argument("--db", default="app.db", help="Path to SQLite DB")
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--log-every", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def open_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_openai_client() -> OpenAI:
    settings = get_settings()
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY missing")
    return OpenAI(api_key=settings.openai_api_key)


def main() -> int:
    args = parse_args()
    settings = get_settings()
    db_path = Path(args.db).expanduser()
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    with open_db(db_path) as conn:
        excluded_exists = conn.execute(
            "SELECT name FROM pragma_table_info('documents') WHERE name = 'excluded'"
        ).fetchone()
        excluded_clause = "AND (documents.excluded IS NULL OR documents.excluded = 0)" if excluded_exists else ""
        total = conn.execute(
            f"""
            SELECT COUNT(*) AS count
            FROM chunks
            JOIN documents ON chunks.document_id = documents.id
            WHERE 1=1 {excluded_clause}
            """
        ).fetchone()["count"]

        print(f"Rebuild FAISS: {total} chunks eligible")
        if args.dry_run:
            return 0

        cursor = conn.execute(
            f"""
            SELECT chunks.id, chunks.chunk_text
            FROM chunks
            JOIN documents ON chunks.document_id = documents.id
            WHERE 1=1 {excluded_clause}
            ORDER BY chunks.id
            """
        )

        client = get_openai_client()
        index = None
        batch = []
        batch_ids = []
        processed = 0
        start_time = time.time()

        def flush_batch() -> None:
            nonlocal index, processed
            if not batch:
                return
            embed_start = time.time()
            embeddings = client.embeddings.create(
                model=settings.openai_embed_model,
                input=batch,
            )
            vectors = np.array([item.embedding for item in embeddings.data]).astype("float32")
            ids = np.array(batch_ids, dtype="int64")

            if index is None:
                dim = vectors.shape[1]
                import faiss

                index = faiss.IndexIDMap2(faiss.IndexFlatL2(dim))

            add_vectors(index, vectors, ids)
            processed += len(batch)
            elapsed = time.time() - start_time
            rate = (processed / elapsed) * 60 if elapsed else 0
            print(
                f"EMBED batch={len(batch)} ms={int((time.time() - embed_start) * 1000)} "
                f"processed={processed}/{total} rate={rate:.1f}/min",
                flush=True,
            )

        for row in cursor:
            batch.append(row["chunk_text"])
            batch_ids.append(row["id"])
            if len(batch) >= args.batch_size:
                flush_batch()
                batch = []
                batch_ids = []

        flush_batch()

    if index is None:
        print("No vectors created. Exiting.")
        return 0

    temp_path = settings.faiss_index_path.with_suffix(".index.tmp")
    save_index(index, temp_path)
    os.replace(temp_path, settings.faiss_index_path)

    summary = {
        "chunks_indexed": processed,
        "faiss_path": str(settings.faiss_index_path),
    }
    print(f"REBUILD DONE {json.dumps(summary, ensure_ascii=True)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
