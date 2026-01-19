import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import app.main as main
from app.settings import Settings


class FakeEvent:
    def __init__(self, delta: str) -> None:
        self.type = "response.output_text.delta"
        self.delta = delta


class FakeResponses:
    def create(self, **kwargs):
        text = "Trip summary [1]\nDay 1: Morning in town. https://example.com/source"
        return iter([FakeEvent(text)])


class FakeClient:
    def __init__(self) -> None:
        self.responses = FakeResponses()


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "app.db"
    data_dir = tmp_path / "data"
    settings = Settings(
        project_root=tmp_path,
        data_dir=data_dir,
        db_path=db_path,
        faiss_index_path=data_dir / "faiss.index",
        openai_api_key="test",
        openai_model="gpt-4.1-mini",
        openai_embed_model="text-embedding-3-small",
        admin_token="token",
        allowed_origins=[],
        geolite2_city_db_path=None,
    )

    monkeypatch.setattr(main, "settings", settings)
    monkeypatch.setattr(main, "get_openai_client", lambda: FakeClient())
    monkeypatch.setattr(main, "retrieve_chunks", lambda *args, **kwargs: [
        {
            "chunk_text": "Sample chunk",
            "heading": None,
            "title": "Sample",
            "url": "https://example.com/source",
            "source_type": "nmmag",
        }
    ])

    from app import db as db_module

    monkeypatch.setattr(db_module, "get_settings", lambda: settings)
    db_module.init_db()

    return TestClient(main.app)


def test_chat_stream_contains_citations(client):
    response = client.post("/api/chat", json={"message": "Plan a trip"})
    assert response.status_code == 200
    body = response.text
    assert "[1]" in body
    assert "https://example.com/source" in body


def test_chat_events_has_no_ip_columns(client, tmp_path):
    conn = sqlite3.connect(tmp_path / "app.db")
    rows = conn.execute("PRAGMA table_info(chat_events)").fetchall()
    columns = [row[1] for row in rows]
    assert not any("ip" in name.lower() for name in columns)
