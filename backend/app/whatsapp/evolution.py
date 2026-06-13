"""
Thin async client for Evolution API (WhatsApp bridge).
All intelligence lives in the backend; this module is just an HTTP transport.
"""

import logging
import os

import httpx

log = logging.getLogger(__name__)

_URL = os.getenv("EVOLUTION_API_URL", "").rstrip("/")
_INSTANCE = os.getenv("EVOLUTION_INSTANCE", "default")
_API_KEY = os.getenv("EVOLUTION_API_KEY", "")


async def send_message(group_jid: str, text: str) -> None:
    if not _URL:
        log.warning("whatsapp_send_skipped", extra={"reason": "EVOLUTION_API_URL not configured"})
        return
    url = f"{_URL}/message/sendText/{_INSTANCE}"
    headers = {"apikey": _API_KEY, "Content-Type": "application/json"}
    payload = {"number": group_jid, "text": text}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
            resp.raise_for_status()
        log.info("whatsapp_message_sent", extra={"jid": group_jid, "chars": len(text)})
    except Exception as exc:
        log.error("whatsapp_send_error", extra={"jid": group_jid, "error": str(exc)})
        raise
