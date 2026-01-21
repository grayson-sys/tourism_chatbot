from __future__ import annotations

import hashlib
from datetime import datetime
from pathlib import Path

import numpy as np
from openai import OpenAI

from app.db import get_conn, update_ingest_progress
from app.rag.chunk import chunk_text
from app.rag.crawl import crawl, load_yaml_list
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


def ingest_urls(seeds: list[str], run_id: int | None = None) -> dict[str, int]:
    settings = get_settings()
    allowlist = load_yaml_list(settings.project_root / "config" / "urls_allowlist.yaml")
    denylist = load_yaml_list(settings.project_root / "config" / "urls_denylist.yaml")

    client = _get_openai_client()
    index = None

    inserted_docs = 0
    updated_docs = 0
    inserted_chunks = 0
    pages_crawled = 0
    documents_seen = 0

    with get_conn() as conn:
        for doc in crawl(seeds, allowlist, denylist, max_pages=settings.crawl_max_pages):
            pages_crawled += 1
            documents_seen += 1
            url = doc["url"]
            title = doc["title"]
            content_text = doc["content_text"]
            content_hash = _hash_text(content_text)
            source_type = _infer_source_type(url)
            published_date = doc.get("published_date")
            image_url = doc.get("image_url")

            existing = conn.execute(
                "SELECT id, content_hash, published_date, image_url FROM documents WHERE url = ?", (url,)
            ).fetchone()

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

            chunks = chunk_text(content_text)
            if not chunks:
                continue

            chunk_texts = []
            chunk_ids = []
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
    }
