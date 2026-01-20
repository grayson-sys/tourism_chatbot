import json
import time
from functools import lru_cache
from typing import Any, Iterable
import re

from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from openai import OpenAI

from app.db import (
    create_ingest_run,
    finish_ingest_run,
    get_conn,
    init_db,
    insert_chat_event,
    latest_ingest_run,
    recent_queries,
)
from app.rag.ingest import DEFAULT_SEEDS, ingest_urls
from app.rag.retrieve import retrieve_chunks
from app.settings import get_settings

try:
    import geoip2.database
except Exception:  # pragma: no cover - optional dependency at runtime
    geoip2 = None


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield
    reader = _get_geo_reader()
    if reader:
        reader.close()


settings = get_settings()
app = FastAPI(lifespan=lifespan)
init_db()
templates = Jinja2Templates(directory=str(settings.project_root / "app" / "templates"))
app.mount("/static", StaticFiles(directory=str(settings.project_root / "app" / "static")), name="static")

if settings.allowed_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allowed_origins,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )


def get_openai_client() -> OpenAI:
    if not settings.openai_api_key:
        raise HTTPException(status_code=500, detail="OPENAI_API_KEY missing")
    return OpenAI(api_key=settings.openai_api_key)


def _get_client_ip(request: Request) -> str | None:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    xri = request.headers.get("x-real-ip")
    if xri:
        return xri.strip()
    if request.client:
        return request.client.host
    return None


@lru_cache
def _get_geo_reader():
    if not settings.geolite2_city_db_path:
        return None
    if not settings.geolite2_city_db_path.exists():
        return None
    if geoip2 is None:
        return None
    return geoip2.database.Reader(str(settings.geolite2_city_db_path))


def _geo_from_ip(ip: str | None) -> tuple[str | None, str | None, str | None]:
    if not ip or not settings.geolite2_city_db_path:
        return None, None, None
    if not settings.geolite2_city_db_path.exists():
        return None, None, None
    if geoip2 is None:
        return None, None, None
    try:
        reader = _get_geo_reader()
        if not reader:
            return None, None, None
        response = reader.city(ip)
        city = response.city.name
        region = response.subdivisions.most_specific.name
        country = response.country.name
        return city, region, country
    except Exception:
        return None, None, None


def _build_system_prompt(trip_length_days: int | None, assumed_long_weekend: bool) -> str:
    if assumed_long_weekend:
        default_line = (
            "If the user does not specify a trip length, assume a long weekend: "
            "Friday night, Saturday, and Sunday."
        )
    else:
        default_line = (
            f"Trip length is {trip_length_days} days unless the user specifies otherwise."
            if trip_length_days
            else "Do not assume a trip length unless the user specifies one."
        )
    return (
        "You are the New Mexico Concierge for NewMexico.org. "
        "Follow these safety and grounding rules:\n"
        "- Never invent business names, addresses, phone numbers, prices, or operating hours.\n"
        "- Only name a specific vendor, hotel, attraction, or tour if supported by retrieved sources.\n"
        "- Lodging suggestions should be areas to stay by default; only name specific hotels if they appear in sources.\n"
        "- Be respectful of New Mexico and avoid disparaging statements about the state or its communities.\n"
        "- Do not output HTML tags; write plain text only.\n"
        "- Include relevant source links at the end of each paragraph when making specific recommendations.\n"
        "- When sources are available, prefer specific businesses, hotels, restaurants, and attractions from those sources over general advice.\n"
        "- Only include a specific recommendation if it is supported by a source URL; otherwise say sources were limited and ask one clarifying question.\n"
        "- Always add a short reminder to check with businesses and events directly because archives can be out of date.\n"
        "- If retrieval is weak, say so plainly, ask at most one clarifying question, and still produce a best-effort itinerary with general region-based guidance.\n"
        "- Prefer New Mexico True Certified vendors for shopping and experiences when relevant.\n\n"
        "Return the response in this structure:\n"
        "Trip summary\n"
        "Day-by-day itinerary (morning / afternoon / evening blocks for each day)\n"
        "New Mexico True Certified picks (shopping and experiences)\n"
        "Practical notes\n\n"
        f"{default_line}"
    )


_ILLEGAL_PATTERNS = [
    r"\bprostitut(e|ion|es)\b",
    r"\bes?cort(s)?\b",
    r"\bsex\s*work(er|ers)?\b",
    r"\bdrugs?\b",
    r"\bcocaine\b",
    r"\bheroin\b",
    r"\bmeth\b",
    r"\bopioids?\b",
    r"\bweed\b",
    r"\bmarijuana\b",
    r"\bketamine\b",
    r"\blsd\b",
    r"\bmdma\b",
    r"\bwhere\s+to\s+buy\b",
    r"\bbuy\s+drugs?\b",
    r"\bhow\s+to\s+get\b.*\b(fake|forged)\b",
]


def _detect_illegal_request(message: str) -> bool:
    lowered = message.lower()
    return any(re.search(pattern, lowered) for pattern in _ILLEGAL_PATTERNS)


def _extract_trip_length(message: str) -> int | None:
    match = re.search(r"\b(\d{1,2})\s*(day|days|night|nights)\b", message.lower())
    if not match:
        return None
    value = int(match.group(1))
    if value <= 0 or value > 21:
        return None
    return value


def _illegal_response() -> str:
    return (
        "Bless your heart, I don't think this is the right place for that. "
        "Breaking Bad was a show and not real life.\n\n"
        "If you want help planning a New Mexico trip, I can do that."
    )


def _build_user_payload(message: str, metadata: dict[str, Any], sources: list[dict[str, Any]]) -> str:
    retrieval_note = None
    if not sources:
        retrieval_note = "No sources retrieved."
    elif len(sources) < 3:
        retrieval_note = "Limited sources retrieved; be cautious and ask one clarifying question."
    payload = {
        "user_request": message,
        "trip_context": metadata,
        "retrieval_note": retrieval_note,
        "retrieved_count": len(sources),
        "sources": [
            {
                "id": idx + 1,
                "title": source["title"],
                "url": source["url"],
                "source_type": source["source_type"],
                "excerpt": source["chunk_text"],
                "image_url": source.get("image_url"),
            }
            for idx, source in enumerate(sources)
        ],
    }
    return json.dumps(payload, ensure_ascii=True, indent=2)


def _approx_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, len(text) // 4)



@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    recent = recent_queries(10)
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "default_days": 7,
            "recent_queries": recent,
        },
    )


@app.get("/privacy", response_class=HTMLResponse)
async def privacy(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("privacy.html", {"request": request})


@app.get("/healthz", response_class=PlainTextResponse)
async def healthz() -> PlainTextResponse:
    return PlainTextResponse("ok")


def _require_admin(request: Request) -> None:
    token = request.headers.get("authorization") or request.query_params.get("token")
    if not settings.admin_token:
        raise HTTPException(status_code=500, detail="ADMIN_TOKEN missing")
    if token == settings.admin_token:
        return
    if token == f"Bearer {settings.admin_token}":
        return
    raise HTTPException(status_code=403, detail="forbidden")


@app.get("/admin/ingest", response_class=HTMLResponse)
async def ingest_status(request: Request) -> HTMLResponse:
    _require_admin(request)
    with get_conn() as conn:
        docs = conn.execute("SELECT COUNT(*) AS count FROM documents").fetchone()["count"]
        chunks = conn.execute("SELECT COUNT(*) AS count FROM chunks").fetchone()["count"]
        updated = conn.execute("SELECT MAX(updated_at) AS latest FROM documents").fetchone()["latest"]
    latest_run = latest_ingest_run()
    return templates.TemplateResponse(
        "ingest_status.html",
        {
            "request": request,
            "documents": docs,
            "chunks": chunks,
            "latest_update": updated,
            "latest_run": latest_run,
        },
    )


@app.post("/api/chat")
async def chat(request: Request) -> StreamingResponse:
    payload = await request.json()
    message = (payload.get("message") or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message is required")
    if _detect_illegal_request(message):
        response_text = _illegal_response()
        latency_ms = 0
        ip = _get_client_ip(request)
        city, region, country = _geo_from_ip(ip)
        insert_chat_event(
            client_city=city,
            client_region=region,
            client_country=country,
            user_agent=request.headers.get("user-agent", ""),
            message=message,
            response_preview=response_text[:500],
            model=settings.openai_model,
            retrieved_count=0,
            latency_ms=latency_ms,
            input_tokens=_approx_tokens(message),
            output_tokens=_approx_tokens(response_text),
        )
        return StreamingResponse(iter([response_text]), media_type="text/plain")

    trip_length_days = int(payload.get("trip_length_days") or 0) or _extract_trip_length(message)
    assumed_long_weekend = False
    if not trip_length_days:
        trip_length_days = 4
        assumed_long_weekend = True
    metadata = {
        "trip_length_days": trip_length_days,
        "party": payload.get("party"),
        "season": payload.get("season"),
        "origin": payload.get("origin"),
    }

    start = time.perf_counter()
    sources = retrieve_chunks(message, top_k=8)
    system_prompt = _build_system_prompt(trip_length_days, assumed_long_weekend)
    user_payload = _build_user_payload(message, metadata, sources)
    input_tokens = _approx_tokens(system_prompt) + _approx_tokens(user_payload)

    client = get_openai_client()
    response = client.responses.create(
        model=settings.openai_model,
        input=[
            {"role": "system", "content": [{"type": "input_text", "text": system_prompt}]},
            {"role": "user", "content": [{"type": "input_text", "text": user_payload}]},
        ],
        temperature=0.3,
        stream=True,
    )

    full_text: list[str] = []

    def event_stream() -> Iterable[str]:
        nonlocal full_text
        try:
            for event in response:
                if getattr(event, "type", None) == "response.output_text.delta":
                    delta = getattr(event, "delta", "")
                    if delta:
                        full_text.append(delta)
                        yield delta
        finally:
            latency_ms = int((time.perf_counter() - start) * 1000)
            output_tokens = _approx_tokens("".join(full_text))
            ip = _get_client_ip(request)
            city, region, country = _geo_from_ip(ip)
            insert_chat_event(
                client_city=city,
                client_region=region,
                client_country=country,
                user_agent=request.headers.get("user-agent", ""),
                message=message,
                response_preview="".join(full_text)[:500],
                model=settings.openai_model,
                retrieved_count=len(sources),
                latency_ms=latency_ms,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )

    return StreamingResponse(event_stream(), media_type="text/plain")


def _run_ingest(run_id: int, seeds: list[str]) -> None:
    try:
        stats = ingest_urls(seeds, run_id=run_id)
        finish_ingest_run(run_id, "complete", json.dumps(stats, ensure_ascii=True), None)
    except Exception as exc:
        finish_ingest_run(run_id, "failed", None, str(exc))


@app.post("/api/ingest")
async def ingest(request: Request, background_tasks: BackgroundTasks) -> dict[str, Any]:
    _require_admin(request)
    payload = await request.json()
    seeds = payload.get("seeds") or DEFAULT_SEEDS
    run_id = create_ingest_run()
    background_tasks.add_task(_run_ingest, run_id, seeds)
    return {"status": "queued", "run_id": run_id}


@app.post("/api/source-images")
async def source_images(request: Request) -> dict[str, Any]:
    payload = await request.json()
    urls = payload.get("urls") or []
    if not isinstance(urls, list):
        raise HTTPException(status_code=400, detail="urls must be a list")
    if not urls:
        return {"images": []}
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT url, title, image_url
            FROM documents
            WHERE url IN ({})
            """.format(",".join("?" for _ in urls)),
            urls,
        ).fetchall()
    images = [
        {"url": row["url"], "title": row["title"], "image_url": row["image_url"]}
        for row in rows
        if row["image_url"]
    ]
    return {"images": images}
