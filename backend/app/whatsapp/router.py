"""
WhatsApp bot routes:
  POST /whatsapp/webhook        — Evolution API → backend (server-to-server)
  GET  /api/whatsapp/groups
  GET  /api/whatsapp/todos
  POST /api/whatsapp/todos
  POST /api/whatsapp/todos/{id}/done
  GET  /api/whatsapp/goals
  POST /api/whatsapp/goals
  POST /api/whatsapp/goals/{id}/done
  GET  /api/whatsapp/reminders
  POST /api/whatsapp/recap
"""

import logging
import os
import re
import time
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request
from pydantic import BaseModel

from . import ai, db, evolution

log = logging.getLogger(__name__)

router = APIRouter()

_WEBHOOK_SECRET = os.getenv("WHATSAPP_WEBHOOK_SECRET", "")

# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_duration(s: str) -> int | None:
    """Parse '2h', '30m', '1d', '1w' → seconds. Returns None on failure."""
    m = re.fullmatch(r"(\d+)\s*(m|h|d|w)", s.strip().lower())
    if not m:
        return None
    value, unit = int(m.group(1)), m.group(2)
    return value * {"m": 60, "h": 3600, "d": 86400, "w": 604800}[unit]


def _extract_text(data: dict) -> str | None:
    msg = data.get("message", {})
    return (
        msg.get("conversation")
        or msg.get("extendedTextMessage", {}).get("text")
    )


async def _handle_command(text: str, group_jid: str, sender_name: str | None) -> None:
    """Parse and execute a ! command, sending the reply back to the group."""
    parts = text.strip().split(None, 2)
    cmd = parts[0].lower()

    if cmd == "!recap":
        messages = db.get_recent_messages(group_jid, limit=100)
        recap = await ai.generate_recap(messages)
        await evolution.send_message(group_jid, f"*Recap*\n\n{recap}")

    elif cmd == "!todo":
        sub = parts[1].lower() if len(parts) > 1 else ""
        rest = parts[2] if len(parts) > 2 else (parts[1] if len(parts) > 1 and sub not in ("list",) else "")

        if sub == "list":
            todos = db.get_todos(group_jid, done=False)
            if not todos:
                await evolution.send_message(group_jid, "No open todos.")
                return
            lines = [f"{t['id']}. {t['text']}" + (f" (@{t['owner']})" if t["owner"] else "") for t in todos]
            await evolution.send_message(group_jid, "*Open todos:*\n" + "\n".join(lines))

        elif sub == "done":
            try:
                todo_id = int(rest.strip())
                if db.set_todo_done(todo_id):
                    await evolution.send_message(group_jid, f"Todo #{todo_id} marked done.")
                else:
                    await evolution.send_message(group_jid, f"Todo #{todo_id} not found.")
            except ValueError:
                await evolution.send_message(group_jid, "Usage: !todo done <id>")

        elif sub:
            # !todo <text> — treat the whole rest of the message as the task text
            task_text = text[len("!todo"):].strip()
            # try to extract @mention as owner
            owner_match = re.search(r"@(\w+)", task_text)
            owner = owner_match.group(1) if owner_match else None
            todo = db.create_todo(task_text, group_jid, owner=owner)
            await evolution.send_message(group_jid, f"Todo added (#{todo['id']}): {task_text}")

        else:
            await evolution.send_message(group_jid, "Usage: !todo <task> | !todo list | !todo done <id>")

    elif cmd == "!remind":
        # !remind <text> in <duration>   e.g.  !remind Stand-up in 2h
        body = text[len("!remind"):].strip()
        in_match = re.search(r"\bin\s+(\S+)\s*$", body, re.IGNORECASE)
        if in_match:
            duration_str = in_match.group(1)
            reminder_text = body[: in_match.start()].strip()
            secs = _parse_duration(duration_str)
        else:
            reminder_text = body
            secs = 86400  # default: 24 hours

        if not reminder_text:
            await evolution.send_message(group_jid, "Usage: !remind <text> in <2h|1d|…>")
            return

        if secs is None:
            await evolution.send_message(group_jid, f"Couldn't parse duration '{in_match.group(1) if in_match else ''}'. Use: 30m, 2h, 1d, 1w")
            return

        due_at = int(time.time()) + secs
        rem = db.create_reminder(reminder_text, group_jid, due_at, created_by=sender_name)
        human_time = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(due_at))
        await evolution.send_message(group_jid, f"Reminder set (#{rem['id']}): \"{reminder_text}\" — due {human_time}")

    elif cmd == "!goal":
        sub = parts[1].lower() if len(parts) > 1 else ""

        if sub == "list":
            goals = db.get_goals(group_jid, done=False)
            if not goals:
                await evolution.send_message(group_jid, "No active goals.")
                return
            lines = [f"{g['id']}. {g['title']}" for g in goals]
            await evolution.send_message(group_jid, "*Active goals:*\n" + "\n".join(lines))

        elif sub == "done":
            rest_text = parts[2] if len(parts) > 2 else ""
            try:
                goal_id = int(rest_text.strip())
                if db.set_goal_done(goal_id):
                    await evolution.send_message(group_jid, f"Goal #{goal_id} marked done.")
                else:
                    await evolution.send_message(group_jid, f"Goal #{goal_id} not found.")
            except ValueError:
                await evolution.send_message(group_jid, "Usage: !goal done <id>")

        elif sub:
            title = text[len("!goal"):].strip()
            goal = db.create_goal(title, group_jid, created_by=sender_name)
            await evolution.send_message(group_jid, f"Goal added (#{goal['id']}): {title}")

        else:
            await evolution.send_message(group_jid, "Usage: !goal <title> | !goal list | !goal done <id>")

    else:
        await evolution.send_message(group_jid, "Unknown command. Available: !recap !todo !remind !goal")


async def _run_passive_extraction(group_jid: str) -> None:
    unprocessed = db.get_unprocessed_messages(group_jid)
    if len(unprocessed) < 10:
        return
    todos = await ai.extract_todos(unprocessed)
    for item in todos:
        text = item.get("text", "").strip()
        owner = item.get("owner")
        if text:
            db.create_todo(text, group_jid, owner=owner)
            log.info("passive_todo_created", extra={"group_jid": group_jid, "text": text[:60]})
    db.mark_processed([m["message_id"] for m in unprocessed])


# ── Webhook ───────────────────────────────────────────────────────────────────

@router.post("/whatsapp/webhook")
async def webhook(request: Request, background_tasks: BackgroundTasks):
    if _WEBHOOK_SECRET:
        if request.headers.get("x-webhook-secret") != _WEBHOOK_SECRET:
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    body = await request.json()
    event = body.get("event", "")

    if event != "messages.upsert":
        return {"status": "ignored"}

    data = body.get("data", {})
    key = data.get("key", {})

    if key.get("fromMe"):
        return {"status": "ignored"}

    group_jid: str = key.get("remoteJid", "")
    if not group_jid.endswith("@g.us"):
        return {"status": "ignored"}  # only handle group messages

    message_id: str = key.get("id", "")
    sender_jid: str = key.get("participant", "") or key.get("remoteJid", "")
    sender_name: str | None = data.get("pushName")
    text = _extract_text(data)
    ts = int(data.get("messageTimestamp", time.time()))

    if not text:
        return {"status": "ignored"}

    saved = db.save_message(message_id, group_jid, sender_jid, sender_name, text, ts)
    if not saved:
        return {"status": "duplicate"}

    log.info("whatsapp_message_received", extra={"group_jid": group_jid, "sender": sender_name})

    if text.startswith("!"):
        background_tasks.add_task(_handle_command, text, group_jid, sender_name)
    else:
        background_tasks.add_task(_run_passive_extraction, group_jid)

    return {"status": "ok"}


# ── REST API for frontend ─────────────────────────────────────────────────────

@router.get("/api/whatsapp/groups")
async def list_groups():
    return db.get_known_groups()


@router.get("/api/whatsapp/todos")
async def list_todos(group_jid: str, done: Optional[bool] = None):
    return db.get_todos(group_jid, done=done)


class TodoCreate(BaseModel):
    group_jid: str
    text: str
    owner: Optional[str] = None


@router.post("/api/whatsapp/todos")
async def create_todo(body: TodoCreate):
    return db.create_todo(body.text, body.group_jid, owner=body.owner)


@router.post("/api/whatsapp/todos/{todo_id}/done")
async def complete_todo(todo_id: int):
    if not db.set_todo_done(todo_id):
        raise HTTPException(status_code=404, detail="Todo not found")
    return {"status": "ok"}


@router.get("/api/whatsapp/goals")
async def list_goals(group_jid: str, done: Optional[bool] = None):
    return db.get_goals(group_jid, done=done)


class GoalCreate(BaseModel):
    group_jid: str
    title: str
    description: Optional[str] = None
    target_date: Optional[int] = None
    created_by: Optional[str] = None


@router.post("/api/whatsapp/goals")
async def create_goal(body: GoalCreate):
    return db.create_goal(
        body.title, body.group_jid,
        description=body.description,
        target_date=body.target_date,
        created_by=body.created_by,
    )


@router.post("/api/whatsapp/goals/{goal_id}/done")
async def complete_goal(goal_id: int):
    if not db.set_goal_done(goal_id):
        raise HTTPException(status_code=404, detail="Goal not found")
    return {"status": "ok"}


@router.get("/api/whatsapp/reminders")
async def list_reminders(group_jid: str, sent: Optional[bool] = None):
    return db.get_reminders(group_jid, sent=sent)


class RecapRequest(BaseModel):
    group_jid: str


@router.post("/api/whatsapp/recap")
async def trigger_recap(body: RecapRequest):
    messages = db.get_recent_messages(body.group_jid, limit=100)
    recap = await ai.generate_recap(messages)
    return {"recap": recap}
