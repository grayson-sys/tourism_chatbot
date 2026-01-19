# New Mexico Concierge (Mockup)

A production-minded FastAPI + Jinja2 app that renders a New Mexico Concierge chat page and serves a RAG-backed chatbot grounded in New Mexico Magazine archives and New Mexico True Certified listings.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
uvicorn app.main:app --reload
```

Visit `http://localhost:8000`.

## Ingest content

1. Start the app.
2. Set `ADMIN_TOKEN` in your `.env` and export it in your shell.
3. Trigger ingestion:

```bash
export ADMIN_TOKEN=change-me
./scripts/ingest_seed.sh
```

Optional: pass a base URL to the script (e.g. `./scripts/ingest_seed.sh http://localhost:8000`).

Allowlist/denylist controls live in `config/urls_allowlist.yaml` and `config/urls_denylist.yaml`. The allowlist is prefilled for New Mexico Magazine (all pages) and New Mexico True Certified sections. Crawling respects robots.txt (both sites allow crawling with a 2-second delay).

Set `CRAWL_MAX_PAGES` in `.env` to control crawl scope (default 2000).

## Ingest status (admin)

Visit `http://localhost:8000/admin/ingest?token=ADMIN_TOKEN` or send `Authorization: Bearer ADMIN_TOKEN` to view a simple ingestion status page.

## GeoLite2 City database

Set `GEOLITE2_CITY_DB_PATH` to the path of the GeoLite2 City `.mmdb` file. If the file is missing or the setting is empty, the app stores `NULL` for location fields and continues processing requests.

## Deployment notes

- Container-friendly: run with `uvicorn app.main:app --host 0.0.0.0 --port 8000`.
- Persist `app.db` and `data/faiss.index` between deploys.
- Provide `OPENAI_API_KEY`, `OPENAI_MODEL`, `OPENAI_EMBED_MODEL`, `ADMIN_TOKEN`, and optional `ALLOWED_ORIGINS`.

## Brand assets (optional)

Place official logos or images in `app/static/brand/`. The `scripts/fetch_brand_assets.py` helper can download assets once URLs are provided.

## Privacy

Analytics store timestamps, message text, model used, retrieval count, latency, user agent, approximate token counts, and approximate city/region/country. IP addresses are never stored or hashed.

## Cost notes

The default model is `gpt-4.1-mini` for quality. You can switch to a cheaper model via `OPENAI_MODEL` for load testing.
