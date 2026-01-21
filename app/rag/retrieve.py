from __future__ import annotations

from datetime import datetime, timezone

from openai import OpenAI

from app.db import get_conn
from app.settings import get_settings


SHOPPING_TERMS = {
    "shop",
    "shopping",
    "souvenir",
    "gift",
    "store",
    "market",
    "vendor",
    "craft",
    "artisan",
    "experience",
    "tour",
}


def _get_openai_client() -> OpenAI:
    settings = get_settings()
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY missing")
    return OpenAI(api_key=settings.openai_api_key)


def _query_needs_nmtc(query: str) -> bool:
    tokens = set(query.lower().split())
    return bool(tokens & SHOPPING_TERMS)


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(cleaned).astimezone(timezone.utc)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(cleaned[:10]).astimezone(timezone.utc)
    except ValueError:
        return None


def _recency_boost(published_date: str | None) -> float:
    parsed = _parse_date(published_date)
    if not parsed:
        return 0.0
    age_days = (datetime.now(timezone.utc) - parsed).days
    if age_days <= 180:
        return 0.15
    if age_days <= 365:
        return 0.1
    if age_days <= 730:
        return 0.05
    return 0.0


def retrieve_chunks(query: str, top_k: int = 8) -> list[dict[str, str]]:
    try:
        import faiss
    except Exception:  # pragma: no cover - optional dependency at runtime
        faiss = None

    import numpy as np

    settings = get_settings()
    if not settings.faiss_index_path.exists() or faiss is None:
        return []

    client = _get_openai_client()
    embed = client.embeddings.create(model=settings.openai_embed_model, input=[query])
    vector = np.array([embed.data[0].embedding], dtype="float32")

    index = faiss.read_index(str(settings.faiss_index_path))
    distances, ids = index.search(vector, top_k)
    ids_list = [int(item) for item in ids[0] if int(item) != -1]
    if not ids_list:
        return []

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT chunks.id, chunks.chunk_text, chunks.heading, documents.title, documents.url, documents.source_type, documents.image_url, documents.published_date
            FROM chunks
            JOIN documents ON chunks.document_id = documents.id
            WHERE chunks.id IN ({})
            """.format(",".join("?" for _ in ids_list)),
            ids_list,
        ).fetchall()

    row_map = {row["id"]: row for row in rows}
    needs_nmtc = _query_needs_nmtc(query)

    scored = []
    for rank, chunk_id in enumerate(ids_list):
        row = row_map.get(chunk_id)
        if not row:
            continue
        score = -float(distances[0][rank])
        if needs_nmtc and row["source_type"] == "nmtc":
            score += 0.2
        if not needs_nmtc and row["source_type"] == "nmmag":
            score += 0.1
        score += _recency_boost(row["published_date"])
        scored.append((score, row))

    scored.sort(key=lambda item: item[0], reverse=True)
    results: list[dict[str, str]] = []
    for _, row in scored:
        results.append(
            {
                "chunk_text": row["chunk_text"],
                "heading": row["heading"],
                "title": row["title"],
                "url": row["url"],
                "source_type": row["source_type"],
                "image_url": row["image_url"],
                "published_date": row["published_date"],
            }
        )
    return results
