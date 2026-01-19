import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("\"").strip("'")
        os.environ.setdefault(key, value)


@dataclass(frozen=True)
class Settings:
    project_root: Path
    data_dir: Path
    db_path: Path
    faiss_index_path: Path
    openai_api_key: str | None
    openai_model: str
    openai_embed_model: str
    admin_token: str | None
    allowed_origins: list[str]
    geolite2_city_db_path: Path | None
    crawl_max_pages: int


@lru_cache
def get_settings() -> Settings:
    project_root = Path(__file__).resolve().parents[1]
    _load_env_file(project_root / ".env")
    data_dir = project_root / "data"
    db_path = project_root / "app.db"
    faiss_index_path = data_dir / "faiss.index"

    allowed_origins_raw = os.getenv("ALLOWED_ORIGINS", "")
    allowed_origins = [item.strip() for item in allowed_origins_raw.split(",") if item.strip()]
    geolite_raw = os.getenv("GEOLITE2_CITY_DB_PATH")
    geolite_path = Path(geolite_raw) if geolite_raw else None

    return Settings(
        project_root=project_root,
        data_dir=data_dir,
        db_path=db_path,
        faiss_index_path=faiss_index_path,
        openai_api_key=os.getenv("OPENAI_API_KEY"),
        openai_model=os.getenv("OPENAI_MODEL", "gpt-4.1-mini"),
        openai_embed_model=os.getenv("OPENAI_EMBED_MODEL", "text-embedding-3-small"),
        admin_token=os.getenv("ADMIN_TOKEN"),
        allowed_origins=allowed_origins,
        geolite2_city_db_path=geolite_path,
        crawl_max_pages=int(os.getenv("CRAWL_MAX_PAGES", "2000")),
    )
