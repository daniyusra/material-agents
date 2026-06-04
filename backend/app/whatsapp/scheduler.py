"""
APScheduler jobs for proactive WhatsApp reminders and weekly recaps.
"""

import logging
import time

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from . import ai, db, evolution

log = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="UTC")


async def _check_reminders() -> None:
    for reminder in db.get_due_reminders():
        try:
            await evolution.send_message(reminder["group_jid"], f"Reminder: {reminder['text']}")
            db.mark_reminder_sent(reminder["id"])
            log.info("reminder_sent", extra={"id": reminder["id"]})
        except Exception as exc:
            log.error("reminder_send_error", extra={"id": reminder["id"], "error": str(exc)})


async def _weekly_recap() -> None:
    cutoff = int(time.time()) - 7 * 24 * 3600
    for group in db.get_known_groups():
        jid = group["group_jid"]
        messages = [m for m in db.get_recent_messages(jid, limit=200) if m["ts"] >= cutoff]
        if not messages:
            continue
        try:
            recap = await ai.generate_recap(messages)
            await evolution.send_message(jid, f"*Weekly Recap*\n\n{recap}")
            log.info("weekly_recap_sent", extra={"group_jid": jid})
        except Exception as exc:
            log.error("weekly_recap_error", extra={"group_jid": jid, "error": str(exc)})


def setup_scheduler() -> AsyncIOScheduler:
    scheduler.add_job(_check_reminders, "interval", minutes=5, id="check_reminders")
    scheduler.add_job(_weekly_recap, "cron", day_of_week="mon", hour=8, minute=0, id="weekly_recap")
    return scheduler
