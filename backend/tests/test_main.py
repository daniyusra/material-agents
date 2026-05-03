import json

import app.main as main_module
from app.agents.data_agent import ChartEvent


async def _fake_stream(messages, provider="anthropic", file_id=None):
    yield "hello"
    yield " world"


async def _chart_stream(messages, provider="anthropic", file_id=None):
    yield "here is your chart"
    yield ChartEvent(figure={"data": [], "layout": {}})


def _parse_sse(text: str) -> list[dict]:
    """Return a list of parsed SSE payloads (each is a dict with 'type' and 'content')."""
    events = []
    for line in text.splitlines():
        if not line.startswith("data: "):
            continue
        payload = line[6:]
        if payload == "[DONE]":
            break
        events.append(json.loads(payload))
    return events


def test_text_events_have_type_field(client, monkeypatch):
    monkeypatch.setattr(main_module, "stream_chat", _fake_stream)
    response = client.post("/api/chat", json={"messages": [{"role": "user", "content": "hi"}]})
    events = _parse_sse(response.text)
    assert all(e["type"] == "text" for e in events)
    assert [e["content"] for e in events] == ["hello", " world"]


def test_chart_event_type_and_content(client, monkeypatch):
    monkeypatch.setattr(main_module, "stream_chat", _chart_stream)
    response = client.post("/api/chat", json={"messages": []})
    events = _parse_sse(response.text)
    text_events = [e for e in events if e["type"] == "text"]
    chart_events = [e for e in events if e["type"] == "chart"]
    assert [e["content"] for e in text_events] == ["here is your chart"]
    assert len(chart_events) == 1
    assert "data" in chart_events[0]["content"]


def test_sse_ends_with_done(client, monkeypatch):
    monkeypatch.setattr(main_module, "stream_chat", _fake_stream)
    response = client.post("/api/chat", json={"messages": []})
    assert "data: [DONE]" in response.text


def test_provider_forwarded(client, monkeypatch):
    received = {}

    async def capture(messages, provider="anthropic", file_id=None):
        received["provider"] = provider
        yield "ok"

    monkeypatch.setattr(main_module, "stream_chat", capture)
    client.post("/api/chat", json={"messages": [], "provider": "openai"})
    assert received["provider"] == "openai"


def test_file_id_forwarded(client, monkeypatch):
    received = {}

    async def capture(messages, provider="anthropic", file_id=None):
        received["file_id"] = file_id
        yield "ok"

    monkeypatch.setattr(main_module, "stream_chat", capture)
    monkeypatch.setattr(main_module, "get_record", lambda fid: object())
    client.post("/api/chat", json={"messages": [], "file_id": "abc-123"})
    assert received["file_id"] == "abc-123"


def test_unknown_file_id_returns_404(client):
    response = client.post("/api/chat", json={"messages": [], "file_id": "does-not-exist"})
    assert response.status_code == 404


def test_invalid_provider_returns_422(client):
    response = client.post("/api/chat", json={"messages": [], "provider": "gemini"})
    assert response.status_code == 422
