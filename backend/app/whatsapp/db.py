"""
SQLite persistence for WhatsApp bot: messages, todos, goals, reminders.
"""

import sqlite3
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

DB_PATH = Path(os.getenv("WHATSAPP_DB_PATH", str(Path(__file__).parent.parent.parent / "whatsapp.db")))


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


@contextmanager
def _db() -> Generator[sqlite3.Connection, None, None]:
    conn = _connect()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    with _db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS wa_messages (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id   TEXT UNIQUE NOT NULL,
                group_jid    TEXT NOT NULL,
                sender_jid   TEXT NOT NULL,
                sender_name  TEXT,
                content      TEXT NOT NULL,
                ts           INTEGER NOT NULL,
                processed    INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS wa_todos (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                text              TEXT NOT NULL,
                owner             TEXT,
                group_jid         TEXT NOT NULL,
                created_at        INTEGER NOT NULL,
                done              INTEGER NOT NULL DEFAULT 0,
                source_message_id TEXT
            );
            CREATE TABLE IF NOT EXISTS wa_goals (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                title       TEXT NOT NULL,
                description TEXT,
                group_jid   TEXT NOT NULL,
                created_at  INTEGER NOT NULL,
                target_date INTEGER,
                done        INTEGER NOT NULL DEFAULT 0,
                created_by  TEXT
            );
            CREATE TABLE IF NOT EXISTS wa_reminders (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                text        TEXT NOT NULL,
                group_jid   TEXT NOT NULL,
                due_at      INTEGER NOT NULL,
                sent        INTEGER NOT NULL DEFAULT 0,
                created_by  TEXT,
                created_at  INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_wa_messages_group ON wa_messages(group_jid, ts);
            CREATE INDEX IF NOT EXISTS idx_wa_todos_group    ON wa_todos(group_jid, done);
            CREATE INDEX IF NOT EXISTS idx_wa_goals_group    ON wa_goals(group_jid, done);
            CREATE INDEX IF NOT EXISTS idx_wa_reminders_due  ON wa_reminders(due_at, sent);
        """)


# ── Messages ──────────────────────────────────────────────────────────────────

def save_message(
    message_id: str,
    group_jid: str,
    sender_jid: str,
    sender_name: str | None,
    content: str,
    ts: int,
) -> bool:
    """Returns False if message_id already exists (dedup)."""
    with _db() as conn:
        try:
            conn.execute(
                """INSERT INTO wa_messages
                       (message_id, group_jid, sender_jid, sender_name, content, ts)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (message_id, group_jid, sender_jid, sender_name, content, ts),
            )
            return True
        except sqlite3.IntegrityError:
            return False


def get_recent_messages(group_jid: str, limit: int = 100) -> list[dict]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM wa_messages WHERE group_jid = ? ORDER BY ts DESC LIMIT ?",
            (group_jid, limit),
        ).fetchall()
    return [dict(r) for r in reversed(rows)]


def get_unprocessed_messages(group_jid: str) -> list[dict]:
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM wa_messages WHERE group_jid = ? AND processed = 0 ORDER BY ts ASC",
            (group_jid,),
        ).fetchall()
    return [dict(r) for r in rows]


def mark_processed(message_ids: list[str]) -> None:
    if not message_ids:
        return
    placeholders = ",".join("?" * len(message_ids))
    with _db() as conn:
        conn.execute(
            f"UPDATE wa_messages SET processed = 1 WHERE message_id IN ({placeholders})",
            message_ids,
        )


def get_known_groups() -> list[dict]:
    with _db() as conn:
        rows = conn.execute(
            """SELECT group_jid, COUNT(*) AS message_count, MAX(ts) AS last_activity
               FROM wa_messages GROUP BY group_jid""",
        ).fetchall()
    return [dict(r) for r in rows]


# ── Todos ─────────────────────────────────────────────────────────────────────

def create_todo(
    text: str,
    group_jid: str,
    owner: str | None = None,
    source_message_id: str | None = None,
) -> dict:
    now = int(time.time())
    with _db() as conn:
        cur = conn.execute(
            """INSERT INTO wa_todos (text, owner, group_jid, created_at, source_message_id)
               VALUES (?, ?, ?, ?, ?)""",
            (text, owner, group_jid, now, source_message_id),
        )
        return {
            "id": cur.lastrowid, "text": text, "owner": owner,
            "group_jid": group_jid, "created_at": now, "done": 0,
        }


def get_todos(group_jid: str, done: bool | None = None) -> list[dict]:
    with _db() as conn:
        if done is None:
            rows = conn.execute(
                "SELECT * FROM wa_todos WHERE group_jid = ? ORDER BY created_at DESC",
                (group_jid,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM wa_todos WHERE group_jid = ? AND done = ? ORDER BY created_at DESC",
                (group_jid, int(done)),
            ).fetchall()
    return [dict(r) for r in rows]


def set_todo_done(todo_id: int, done: bool = True) -> bool:
    with _db() as conn:
        cur = conn.execute("UPDATE wa_todos SET done = ? WHERE id = ?", (int(done), todo_id))
    return cur.rowcount > 0


# ── Goals ─────────────────────────────────────────────────────────────────────

def create_goal(
    title: str,
    group_jid: str,
    description: str | None = None,
    target_date: int | None = None,
    created_by: str | None = None,
) -> dict:
    now = int(time.time())
    with _db() as conn:
        cur = conn.execute(
            """INSERT INTO wa_goals (title, description, group_jid, created_at, target_date, created_by)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (title, description, group_jid, now, target_date, created_by),
        )
        return {
            "id": cur.lastrowid, "title": title, "description": description,
            "group_jid": group_jid, "created_at": now, "target_date": target_date,
            "done": 0, "created_by": created_by,
        }


def get_goals(group_jid: str, done: bool | None = None) -> list[dict]:
    with _db() as conn:
        if done is None:
            rows = conn.execute(
                "SELECT * FROM wa_goals WHERE group_jid = ? ORDER BY created_at DESC",
                (group_jid,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM wa_goals WHERE group_jid = ? AND done = ? ORDER BY created_at DESC",
                (group_jid, int(done)),
            ).fetchall()
    return [dict(r) for r in rows]


def set_goal_done(goal_id: int, done: bool = True) -> bool:
    with _db() as conn:
        cur = conn.execute("UPDATE wa_goals SET done = ? WHERE id = ?", (int(done), goal_id))
    return cur.rowcount > 0


# ── Reminders ─────────────────────────────────────────────────────────────────

def create_reminder(
    text: str,
    group_jid: str,
    due_at: int,
    created_by: str | None = None,
) -> dict:
    now = int(time.time())
    with _db() as conn:
        cur = conn.execute(
            """INSERT INTO wa_reminders (text, group_jid, due_at, created_by, created_at)
               VALUES (?, ?, ?, ?, ?)""",
            (text, group_jid, due_at, created_by, now),
        )
        return {
            "id": cur.lastrowid, "text": text, "group_jid": group_jid,
            "due_at": due_at, "sent": 0, "created_by": created_by, "created_at": now,
        }


def get_reminders(group_jid: str, sent: bool | None = None) -> list[dict]:
    with _db() as conn:
        if sent is None:
            rows = conn.execute(
                "SELECT * FROM wa_reminders WHERE group_jid = ? ORDER BY due_at ASC",
                (group_jid,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM wa_reminders WHERE group_jid = ? AND sent = ? ORDER BY due_at ASC",
                (group_jid, int(sent)),
            ).fetchall()
    return [dict(r) for r in rows]


def get_due_reminders() -> list[dict]:
    now = int(time.time())
    with _db() as conn:
        rows = conn.execute(
            "SELECT * FROM wa_reminders WHERE due_at <= ? AND sent = 0",
            (now,),
        ).fetchall()
    return [dict(r) for r in rows]


def mark_reminder_sent(reminder_id: int) -> None:
    with _db() as conn:
        conn.execute("UPDATE wa_reminders SET sent = 1 WHERE id = ?", (reminder_id,))
