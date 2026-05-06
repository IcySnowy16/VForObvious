from __future__ import annotations

import logging
from datetime import time as dt_time
from typing import Any, Dict
from zoneinfo import ZoneInfo

from telegram.ext import Application, ContextTypes

from .config import get_timezone
from .storage import load_db

logger = logging.getLogger(__name__)

DAILY_JOB = "reminder:daily:{chat_id}:{user_id}"
WEEKLY_JOB = "reminder:weekly:{chat_id}:{user_id}"
DEFAULT_TEXT = "Reminder: review your progress and update your tasks."


async def _send_reminder(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data or {}
    chat_id = data.get("chat_id")
    if chat_id is None:
        return
    text = data.get("text", DEFAULT_TEXT)
    await context.bot.send_message(chat_id=chat_id, text=text)


def schedule_all_reminders(app: Application) -> None:
    if app.job_queue is None:
        return

    db = load_db()
    tz = ZoneInfo(get_timezone())

    for chat_id, chat in db.get("chats", {}).items():
        users = chat.get("users", {}) if isinstance(chat, dict) else {}
        for user_id, user in users.items():
            reminders = user.get("reminders", {}) if isinstance(user, dict) else {}
            apply_user_reminders(app, chat_id, user_id, reminders, tz)


def apply_user_reminders(
    app: Application,
    chat_id: str | int,
    user_id: str | int,
    reminders: Dict[str, Any],
    tz: ZoneInfo | None = None,
) -> None:
    if app.job_queue is None:
        return

    tz = tz or ZoneInfo(get_timezone())
    chat_value = _safe_chat_id(chat_id)
    if chat_value is None:
        return

    _clear_jobs(app, chat_id, user_id)

    daily = reminders.get("daily", {}) if isinstance(reminders, dict) else {}
    weekly = reminders.get("weekly", {}) if isinstance(reminders, dict) else {}

    if daily.get("enabled"):
        daily_time = _parse_time(daily.get("time", ""), tz)
        if daily_time:
            name = DAILY_JOB.format(chat_id=chat_id, user_id=user_id)
            app.job_queue.run_daily(
                _send_reminder,
                time=daily_time,
                name=name,
                data={"chat_id": chat_value, "text": DEFAULT_TEXT},
            )

    if weekly.get("enabled"):
        weekly_time = _parse_time(weekly.get("time", ""), tz)
        weekday = _parse_weekday(weekly.get("weekday"))
        if weekly_time and weekday is not None:
            name = WEEKLY_JOB.format(chat_id=chat_id, user_id=user_id)
            app.job_queue.run_daily(
                _send_reminder,
                time=weekly_time,
                days=(weekday,),
                name=name,
                data={"chat_id": chat_value, "text": DEFAULT_TEXT},
            )


def _clear_jobs(app: Application, chat_id: str | int, user_id: str | int) -> None:
    if app.job_queue is None:
        return

    for name in [
        DAILY_JOB.format(chat_id=chat_id, user_id=user_id),
        WEEKLY_JOB.format(chat_id=chat_id, user_id=user_id),
    ]:
        for job in app.job_queue.get_jobs_by_name(name):
            job.schedule_removal()


def _parse_time(raw: str, tz: ZoneInfo) -> dt_time | None:
    if not isinstance(raw, str) or ":" not in raw:
        return None
    parts = raw.split(":", 1)
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return dt_time(hour=hour, minute=minute, tzinfo=tz)


def _parse_weekday(value: Any) -> int | None:
    if isinstance(value, int):
        return value if 0 <= value <= 6 else None
    try:
        num = int(value)
        return num if 0 <= num <= 6 else None
    except (TypeError, ValueError):
        return None


def _safe_chat_id(chat_id: str | int) -> int | str | None:
    if isinstance(chat_id, int):
        return chat_id
    if isinstance(chat_id, str):
        try:
            return int(chat_id)
        except ValueError:
            return None
    return None
