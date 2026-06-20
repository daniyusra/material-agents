"""
Authentication for the blog admin.

Design:
  - Single admin; credentials in environment only (no DB user table).
  - ADMIN_PASSWORD_HASH is a bcrypt hash — generate with scripts/gen_password_hash.py.
  - Session token: JWT HS256 signed with AUTH_SECRET, stored in an httpOnly cookie.
  - Cookie flags:
      COOKIE_SECURE=true  → SameSite=None; Secure  (production, cross-origin)
      COOKIE_SECURE=false → SameSite=Lax            (dev, same-site via Vite proxy)
  - CSRF: all state-changing endpoints require X-Requested-With: XMLHttpRequest,
      which cross-site form posts cannot include without a CORS preflight we deny.
  - Login: rate-limited (5/min per IP via slowapi), constant 400ms delay on failure,
      generic error message (no username/password hint).
  - Startup: validate_auth_config() is called from lifespan; app refuses to boot
      if AUTH_SECRET < 32 bytes or username/hash are missing.

All env vars are read at call time (not import time) so tests can monkeypatch them.
"""

import asyncio
import logging
import os
import secrets
import time
from typing import Optional

import bcrypt
import jwt
from fastapi import Cookie, Depends, HTTPException, Request, Response
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

log = logging.getLogger(__name__)

_LOGIN_RATE = os.getenv("RATELIMIT_LOGIN", "5/minute")


# ── Config accessors (read at call time for test monkeypatching) ───────────────

def _auth_secret() -> str:
    return os.getenv("AUTH_SECRET", "")

def _admin_username() -> str:
    return os.getenv("ADMIN_USERNAME", "")

def _admin_password_hash() -> str:
    return os.getenv("ADMIN_PASSWORD_HASH", "")

def _session_ttl_hours() -> int:
    return int(os.getenv("SESSION_TTL_HOURS", "12"))

def _cookie_secure() -> bool:
    return os.getenv("COOKIE_SECURE", "true").lower() == "true"


# ── Startup validation ─────────────────────────────────────────────────────────

def validate_auth_config() -> None:
    """Raise ValueError on startup if any required secret is absent or weak."""
    errors: list[str] = []
    if len(_auth_secret()) < 32:
        errors.append("AUTH_SECRET must be at least 32 characters")
    if not _admin_username():
        errors.append("ADMIN_USERNAME is required")
    if not _admin_password_hash():
        errors.append("ADMIN_PASSWORD_HASH is required (run scripts/gen_password_hash.py)")
    if errors:
        raise ValueError("Blog auth misconfigured — refusing to start: " + "; ".join(errors))


# ── Token ─────────────────────────────────────────────────────────────────────

def _create_token() -> str:
    now = int(time.time())
    payload = {"sub": "admin", "iat": now, "exp": now + _session_ttl_hours() * 3600}
    return jwt.encode(payload, _auth_secret(), algorithm="HS256")


def _verify_token(token: str) -> bool:
    try:
        jwt.decode(token, _auth_secret(), algorithms=["HS256"])
        return True
    except jwt.PyJWTError:
        return False


# ── Cookie helpers ─────────────────────────────────────────────────────────────

def _set_session_cookie(response: Response, token: str) -> None:
    secure = _cookie_secure()
    response.set_cookie(
        key="session",
        value=token,
        httponly=True,
        samesite="none" if secure else "lax",
        secure=secure,
        max_age=_session_ttl_hours() * 3600,
        path="/",
    )


def _clear_session_cookie(response: Response) -> None:
    secure = _cookie_secure()
    response.delete_cookie(
        key="session",
        path="/",
        samesite="none" if secure else "lax",
        secure=secure,
    )


# ── FastAPI dependencies ───────────────────────────────────────────────────────

def _get_session(request: Request) -> Optional[str]:
    return request.cookies.get("session")


def require_admin(session: Optional[str] = Depends(_get_session)) -> None:
    """Dependency: rejects unauthenticated requests with HTTP 401."""
    if not session or not _verify_token(session):
        raise HTTPException(status_code=401, detail="Not authenticated")


def optional_admin(session: Optional[str] = Depends(_get_session)) -> bool:
    """Dependency: returns True if request carries a valid session token."""
    return bool(session and _verify_token(session))


def require_csrf(request: Request) -> None:
    """
    CSRF guard: require the X-Requested-With: XMLHttpRequest header.

    Cross-site form POSTs cannot set this header; cross-site XHR needs a
    CORS preflight which we deny for non-whitelisted origins — so only our
    own frontend can satisfy both the cookie and this header together.
    """
    if request.headers.get("x-requested-with", "").lower() != "xmlhttprequest":
        raise HTTPException(status_code=403, detail="CSRF check failed")


# Convenience combined dependency lists for router declarations
AdminAuth = [Depends(require_admin)]
AdminAuthWithCsrf = [Depends(require_admin), Depends(require_csrf)]


# ── Rate limiter (isolated from the main app's limiter) ───────────────────────

blog_limiter = Limiter(key_func=get_remote_address)


# ── Login / logout handlers ────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str


async def handle_login(request: Request, body: LoginRequest, response: Response) -> dict:
    """
    Rate-limited, constant-time login.

    Always runs bcrypt even on username mismatch (prevents timing oracle).
    Returns the same generic message for any failure.
    """
    username_ok = secrets.compare_digest(
        body.username.encode("utf-8"),
        _admin_username().encode("utf-8"),
    )
    try:
        password_ok = bcrypt.checkpw(
            body.password.encode("utf-8"),
            _admin_password_hash().encode("utf-8"),
        )
    except Exception:
        password_ok = False

    if not username_ok or not password_ok:
        log.warning(
            "blog_login_failed",
            extra={"ip": get_remote_address(request), "username_attempt": body.username[:32]},
        )
        await asyncio.sleep(0.4)   # constant penalty — mitigates brute-force timing
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = _create_token()
    _set_session_cookie(response, token)
    log.info("blog_login_success", extra={"ip": get_remote_address(request)})
    return {"ok": True}


def handle_logout(response: Response) -> dict:
    _clear_session_cookie(response)
    return {"ok": True}


def handle_me(session: Optional[str] = Depends(_get_session)) -> dict:
    return {"authenticated": bool(session and _verify_token(session))}
