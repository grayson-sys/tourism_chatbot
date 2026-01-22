from __future__ import annotations

import hashlib
import logging
import time
from datetime import datetime
from pathlib import Path

import numpy as np
from openai import OpenAI

from app.db import get_conn, init_db, update_ingest_progress
from app.rag.chunk import chunk_text
from app.rag.crawl import CrawlStats, crawl, load_yaml_list
from app.rag.index_faiss import add_vectors, load_or_create, save_index
from app.settings import get_settings

DEFAULT_SEEDS = [
    "https://www.newmexicomagazine.org/archive/",
    "https://www.newmexicomagazine.org/",
    "https://www.newmexico.org/",
    "https://www.newmexico.org/new-mexico-true-certified/",
    "https://www.newmexico.org/new-mexico-true-certified/true-certified-shopping/",
    "https://www.newmexico.org/new-mexico-true-certified/true-certified-visitor-experiences/",
]


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _infer_source_type(url: str) -> str:
    url_lower = url.lower()
    if "newmexicomagazine.org" in url_lower:
        return "nmmag"
    if "new-mexico-true-certified" in url_lower:
        return "nmtc"
    return "nmmag"


def _get_openai_client() -> OpenAI:
    settings = get_settings()
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY missing")
    return OpenAI(api_key=settings.openai_api_key)


def ingest_urls(
    seeds: list[str],
    run_id: int | None = None,
    *,
    max_pages: int | None = None,
    rate_limit_seconds: float | None = None,
    log_every: int = 25,
    per_host_cap: int | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, int | dict[str, int]]:
    init_db()
    settings = get_settings()
    allowlist = load_yaml_list(settings.project_root / "config" / "urls_allowlist.yaml")
    denylist = load_yaml_list(settings.project_root / "config" / "urls_denylist.yaml")

    logger = logger or logging.getLogger("ingest")
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)
    client = _get_openai_client()
    index = None

    inserted_docs = 0
    updated_docs = 0
    inserted_chunks = 0
    pages_crawled = 0
    documents_seen = 0
    pages_ingested = 0
    errors_count = 0

    crawl_stats = CrawlStats()
    max_pages = max_pages or settings.crawl_max_pages
    rate_limit_seconds = rate_limit_seconds or 1.5
    start_time = time.time()

    def log_heartbeat(last_doc: dict[str, str]) -> None:
        if pages_ingested % max(1, log_every) != 0:
            return
        elapsed = time.time() - start_time
        rate = (pages_ingested / elapsed) * 60 if elapsed else 0
        logger.info(
            "HEARTBEAT ingested=%s queue=%s elapsed=%.1fs rate=%.1f/min last=%s",
            pages_ingested,
            last_doc.get("_queue_size"),
            elapsed,
            rate,
            last_doc.get("_last_url"),
        )

    with get_conn() as conn:
        for doc in crawl(
            seeds,
            allowlist,
            denylist,
            max_pages=max_pages,
            rate_limit_seconds=rate_limit_seconds,
            log_every=log_every,
            per_host_cap=per_host_cap,
            stats=crawl_stats,
            logger=logger,
        ):
            pages_crawled += 1
            documents_seen += 1
            url = doc["url"]
            title = doc["title"]
            content_text = doc["content_text"]
            content_hash = _hash_text(content_text)
            source_type = _infer_source_type(url)
            published_date = doc.get("published_date")
            image_url = doc.get("image_url")

            try:
                existing = conn.execute(
                    "SELECT id, content_hash, published_date, image_url FROM documents WHERE url = ?",
                    (url,),
                ).fetchone()
            except Exception as exc:
                errors_count += 1
                logger.error("DB READ ERROR %s %s", url, exc)
                continue

            try:
                if existing and existing["content_hash"] == content_hash:
                    needs_meta_update = False
                    updated_fields = {
                        "published_date": existing["published_date"],
                        "image_url": existing["image_url"],
                    }
                    if published_date and not existing["published_date"]:
                        updated_fields["published_date"] = published_date
                        needs_meta_update = True
                    if image_url and not existing["image_url"]:
                        updated_fields["image_url"] = image_url
                        needs_meta_update = True
                    if needs_meta_update:
                        conn.execute(
                            """
                            UPDATE documents
                            SET published_date = ?, image_url = ?, updated_at = ?
                            WHERE id = ?
                            """,
                            (
                                updated_fields["published_date"],
                                updated_fields["image_url"],
                                datetime.utcnow().isoformat(),
                                existing["id"],
                            ),
                        )
                    if run_id:
                        update_ingest_progress(
                            run_id,
                            pages_crawled=pages_crawled,
                            documents_seen=documents_seen,
                            chunks_embedded=inserted_chunks,
                        )
                    pages_ingested += 1
                    log_heartbeat(doc)
                    continue

                now = datetime.utcnow().isoformat()
                if existing:
                    document_id = existing["id"]
                    conn.execute(
                        """
                        UPDATE documents
                        SET title = ?, source_type = ?, published_date = ?, content_text = ?, content_hash = ?, image_url = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            title,
                            source_type,
                            published_date,
                            content_text,
                            content_hash,
                            image_url,
                            now,
                            document_id,
                        ),
                    )
                    updated_docs += 1
                    chunk_rows = conn.execute(
                        "SELECT id FROM chunks WHERE document_id = ?", (document_id,)
                    ).fetchall()
                    chunk_ids = [row["id"] for row in chunk_rows]
                    conn.execute("DELETE FROM chunks WHERE document_id = ?", (document_id,))
                    if chunk_ids and settings.faiss_index_path.exists():
                        import faiss

                        index = faiss.read_index(str(settings.faiss_index_path))
                        if not isinstance(index, faiss.IndexIDMap2):
                            index = faiss.IndexIDMap2(index)
                        index.remove_ids(np.array(chunk_ids, dtype="int64"))
                        save_index(index, settings.faiss_index_path)
                else:
                    cursor = conn.execute(
                        """
                        INSERT INTO documents(url, title, source_type, published_date, content_text, content_hash, image_url, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            url,
                            title,
                            source_type,
                            published_date,
                            content_text,
                            content_hash,
                            image_url,
                            now,
                            now,
                        ),
                    )
                    document_id = cursor.lastrowid
                    inserted_docs += 1
            except Exception as exc:
                errors_count += 1
                logger.error("DB WRITE ERROR %s %s", url, exc)
                continue

            chunks = chunk_text(content_text)
            if not chunks:
                pages_ingested += 1
                log_heartbeat(doc)
                continue

            chunk_texts = []
            chunk_ids = []
            try:
                for idx, chunk in enumerate(chunks):
                    cursor = conn.execute(
                        """
                        INSERT INTO chunks(document_id, chunk_index, heading, chunk_text)
                        VALUES (?, ?, ?, ?)
                        """,
                        (document_id, idx, chunk.heading, chunk.text),
                    )
                    chunk_ids.append(cursor.lastrowid)
                    chunk_texts.append(chunk.text)
                    inserted_chunks += 1
            except Exception as exc:
                errors_count += 1
                logger.error("DB CHUNK ERROR %s %s", url, exc)
                continue

            embedding_started = time.time()
            logger.info("EMBED START chunks=%s url=%s", len(chunk_texts), url)
            try:
                embeddings = client.embeddings.create(
                    model=settings.openai_embed_model,
                    input=chunk_texts,
                )
                vectors = np.array([item.embedding for item in embeddings.data]).astype("float32")
                ids = np.array(chunk_ids, dtype="int64")

                if index is None:
                    dim = vectors.shape[1]
                    index = load_or_create(settings.faiss_index_path, dim)
                add_vectors(index, vectors, ids)
                save_index(index, settings.faiss_index_path)
                logger.info(
                    "EMBED DONE chunks=%s ms=%s url=%s",
                    len(chunk_texts),
                    int((time.time() - embedding_started) * 1000),
                    url,
                )
            except Exception as exc:
                errors_count += 1
                logger.error("EMBED ERROR %s %s", url, exc)
                continue

            pages_ingested += 1
            log_heartbeat(doc)
            if run_id:
                update_ingest_progress(
                    run_id,
                    pages_crawled=pages_crawled,
                    documents_seen=documents_seen,
                    chunks_embedded=inserted_chunks,
                )

    return {
        "documents_inserted": inserted_docs,
        "documents_updated": updated_docs,
        "chunks_inserted": inserted_chunks,
        "pages_fetched": crawl_stats.pages_fetched,
        "pages_ingested": pages_ingested,
        "pages_skipped": crawl_stats.pages_skipped,
        "errors_count": crawl_stats.errors_count + errors_count,
        "timeouts_count": crawl_stats.timeouts_count,
        "robots_blocked_count": crawl_stats.robots_blocked_count,
        "per_status_counts": dict(crawl_stats.per_status_counts),
        "per_host_counts": dict(crawl_stats.per_host_counts),
    }
