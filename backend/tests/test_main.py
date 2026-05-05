import json

import app.main as main_module
from app.agents.data_agent import ChartEvent
from app.main import _user_facing_error


async def _fake_stream(messages, provider="anthropic", file_id=None, api_key=None):
    yield "hello"
    yield " world"


async def _chart_stream(messages, provider="anthropic", file_id=None, api_key=None):
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


# ── Existing SSE format tests ───────────────────────────────────────────────��─

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

    async def capture(messages, provider="anthropic", file_id=None, api_key=None):
        received["provider"] = provider
        yield "ok"

    monkeypatch.setattr(main_module, "stream_chat", capture)
    client.post("/api/chat", json={"messages": [], "provider": "openai"})
    assert received["provider"] == "openai"


def test_file_id_forwarded(client, monkeypatch):
    received = {}

    async def capture(messages, provider="anthropic", file_id=None, api_key=None):
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


# ── api_key forwarding ────────────────────────────────────────────────────────

def test_api_key_forwarded(client, monkeypatch):
    received = {}

    async def capture(messages, provider="anthropic", file_id=None, api_key=None):
        received["api_key"] = api_key
        yield "ok"

    monkeypatch.setattr(main_module, "stream_chat", capture)
    client.post("/api/chat", json={"messages": [], "api_key": "sk-test-key"})
    assert received["api_key"] == "sk-test-key"


def test_api_key_absent_defaults_to_none(client, monkeypatch):
    received = {}

    async def capture(messages, provider="anthropic", file_id=None, api_key=None):
        received["api_key"] = api_key
        yield "ok"

    monkeypatch.setattr(main_module, "stream_chat", capture)
    client.post("/api/chat", json={"messages": []})
    assert received["api_key"] is None


# ── Error SSE event ───────────────────────────────────────────────────────────

def test_stream_exception_yields_error_event(client, monkeypatch):
    async def boom(messages, provider="anthropic", file_id=None, api_key=None):
        yield "partial"
        raise RuntimeError("something went wrong")

    monkeypatch.setattr(main_module, "stream_chat", boom)
    response = client.post("/api/chat", json={"messages": []})
    events = _parse_sse(response.text)
    error_events = [e for e in events if e["type"] == "error"]
    assert len(error_events) == 1
    assert "something went wrong" in error_events[0]["content"]


def test_error_event_still_followed_by_done(client, monkeypatch):
    async def boom(messages, provider="anthropic", file_id=None, api_key=None):
        raise ValueError("bad key")
        yield  # make it an async generator

    monkeypatch.setattr(main_module, "stream_chat", boom)
    response = client.post("/api/chat", json={"messages": []})
    assert "data: [DONE]" in response.text


# ── _user_facing_error ────────────────────────────────────────────────────────

class TestUserFacingError:
    def test_401_in_message(self):
        msg = _user_facing_error(Exception("HTTP 401 Unauthorized"))
        assert "invalid or incorrect" in msg

    def test_authentication_error_class_name(self):
        class AuthenticationError(Exception):
            pass
        msg = _user_facing_error(AuthenticationError("bad key"))
        assert "invalid or incorrect" in msg

    def test_incorrect_api_key_phrase(self):
        msg = _user_facing_error(Exception("Incorrect API key provided"))
        assert "invalid or incorrect" in msg

    def test_invalid_api_key_phrase(self):
        msg = _user_facing_error(Exception("Invalid API key"))
        assert "invalid or incorrect" in msg

    def test_unauthorized_phrase(self):
        msg = _user_facing_error(Exception("unauthorized request"))
        assert "invalid or incorrect" in msg

    def test_rate_limit_429(self):
        msg = _user_facing_error(Exception("429 Too Many Requests"))
        assert "Rate limit" in msg

    def test_rate_limit_error_class_name(self):
        class RateLimitError(Exception):
            pass
        msg = _user_facing_error(RateLimitError("slow down"))
        assert "Rate limit" in msg

    def test_quota_exceeded(self):
        msg = _user_facing_error(Exception("You exceeded your current quota"))
        assert "quota" in msg.lower()

    def test_insufficient_quota(self):
        msg = _user_facing_error(Exception("insufficient_quota on account"))
        assert "quota" in msg.lower()

    def test_generic_error_includes_message(self):
        msg = _user_facing_error(Exception("some unexpected failure"))
        assert "some unexpected failure" in msg

    def test_long_generic_error_is_truncated(self):
        msg = _user_facing_error(Exception("x" * 500))
        assert len(msg) < 400
