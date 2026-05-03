import json

import app.main as main_module


async def _fake_stream(messages, provider="anthropic", file_id=None):
    yield "hello"
    yield " world"


def _parse_sse(text: str) -> list[str]:
    chunks = []
    for line in text.splitlines():
        if not line.startswith("data: "):
            continue
        payload = line[6:]
        if payload == "[DONE]":
            break
        chunks.append(json.loads(payload)["content"])
    return chunks


def test_chat_streams_content(client, monkeypatch):
    monkeypatch.setattr(main_module, "stream_chat", _fake_stream)
    response = client.post(
        "/api/chat",
        json={"messages": [{"role": "user", "content": "hi"}]},
    )
    assert response.status_code == 200
    assert _parse_sse(response.text) == ["hello", " world"]


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
    # bypass the file-not-found check by also patching get_record
    monkeypatch.setattr(main_module, "get_record", lambda fid: object())
    client.post("/api/chat", json={"messages": [], "file_id": "abc-123"})
    assert received["file_id"] == "abc-123"


def test_unknown_file_id_returns_404(client):
    response = client.post(
        "/api/chat",
        json={"messages": [], "file_id": "does-not-exist"},
    )
    assert response.status_code == 404


def test_invalid_provider_returns_422(client):
    response = client.post(
        "/api/chat",
        json={"messages": [], "provider": "gemini"},
    )
    assert response.status_code == 422
