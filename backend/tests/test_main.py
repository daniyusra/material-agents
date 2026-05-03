import json

import app.main as main_module


async def _fake_stream(messages):
    yield "hello"
    yield " world"


def _parse_sse(text: str) -> list[str]:
    """Extract content values from an SSE response body."""
    chunks = []
    for line in text.splitlines():
        if not line.startswith("data: "):
            continue
        payload = line[6:]
        if payload == "[DONE]":
            break
        chunks.append(json.loads(payload)["content"])
    return chunks


def test_anthropic_provider_streams_content(client, monkeypatch):
    monkeypatch.setitem(main_module._providers, "anthropic", _fake_stream)
    response = client.post(
        "/api/chat",
        json={"messages": [{"role": "user", "content": "hi"}], "provider": "anthropic"},
    )
    assert response.status_code == 200
    assert _parse_sse(response.text) == ["hello", " world"]


def test_openai_provider_streams_content(client, monkeypatch):
    monkeypatch.setitem(main_module._providers, "openai", _fake_stream)
    response = client.post(
        "/api/chat",
        json={"messages": [{"role": "user", "content": "hi"}], "provider": "openai"},
    )
    assert response.status_code == 200
    assert _parse_sse(response.text) == ["hello", " world"]


def test_default_provider_is_anthropic(client, monkeypatch):
    called_with = {}

    async def recording_stream(messages):
        called_with["messages"] = messages
        yield "ok"

    monkeypatch.setitem(main_module._providers, "anthropic", recording_stream)
    client.post("/api/chat", json={"messages": [{"role": "user", "content": "ping"}]})
    assert called_with.get("messages") is not None


def test_sse_ends_with_done(client, monkeypatch):
    monkeypatch.setitem(main_module._providers, "anthropic", _fake_stream)
    response = client.post(
        "/api/chat",
        json={"messages": [{"role": "user", "content": "hi"}]},
    )
    assert "data: [DONE]" in response.text


def test_invalid_provider_returns_422(client):
    response = client.post(
        "/api/chat",
        json={"messages": [], "provider": "gemini"},
    )
    assert response.status_code == 422


def test_messages_passed_through_to_stream(client, monkeypatch):
    received = {}

    async def capture(messages):
        received["messages"] = messages
        yield "x"

    monkeypatch.setitem(main_module._providers, "anthropic", capture)
    payload = [{"role": "user", "content": "test message"}]
    client.post("/api/chat", json={"messages": payload, "provider": "anthropic"})
    assert received["messages"] == payload
