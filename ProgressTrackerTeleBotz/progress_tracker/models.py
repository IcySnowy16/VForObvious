from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

STATUS_TODO = "todo"
STATUS_DOING = "doing"
STATUS_DONE = "done"
STATUS_BLOCKED = "blocked"

TASK_KIND_TASK = "task"
TASK_KIND_EXPERIMENT = "experiment"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def new_db() -> Dict[str, Any]:
    now = now_iso()
    return {
        "version": 1,
        "created_at": now,
        "updated_at": now,
        "chats": {},
    }


def default_user_settings() -> Dict[str, Any]:
    # milestone_positions are display markers for the progress bar.
    return {
        "milestone_positions": None,
        "symbols": {},
        "emoji": {},
        "bar_segments": None,
    }


def default_reminders() -> Dict[str, Any]:
    return {
        "daily": {"enabled": False, "time": "20:00"},
        "weekly": {"enabled": False, "weekday": 0, "time": "09:00"},
    }


def new_user_data() -> Dict[str, Any]:
    now = now_iso()
    return {
        "created_at": now,
        "updated_at": now,
        "settings": default_user_settings(),
        "reminders": default_reminders(),
        "goals": {},
        "skills": {},
        "stages": {},
        "milestones": {},
        "scopes": {},
        "tasks": {},
        "insights": {},
    }


def ensure_chat(db: Dict[str, Any], chat_id: str | int) -> Dict[str, Any]:
    chats = db.setdefault("chats", {})
    key = str(chat_id)
    if key not in chats:
        chats[key] = {"users": {}}
    return chats[key]


def ensure_user(db: Dict[str, Any], chat_id: str | int, user_id: str | int) -> Dict[str, Any]:
    chat = ensure_chat(db, chat_id)
    users = chat.setdefault("users", {})
    key = str(user_id)
    if key not in users:
        users[key] = new_user_data()
    return users[key]


def touch(item: Dict[str, Any]) -> None:
    item["updated_at"] = now_iso()


def new_goal(name: str, description: str = "") -> Dict[str, Any]:
    now = now_iso()
    return {
        "id": make_id("goal"),
        "name": name,
        "description": description,
        "status": STATUS_TODO,
        "created_at": now,
        "updated_at": now,
        "skill_ids": [],
    }


def new_skill(goal_id: str, name: str, description: str = "") -> Dict[str, Any]:
    now = now_iso()
    return {
        "id": make_id("skill"),
        "goal_id": goal_id,
        "name": name,
        "description": description,
        "status": STATUS_TODO,
        "created_at": now,
        "updated_at": now,
        "stage_ids": [],
    }


def new_stage(skill_id: str, name: str, description: str = "") -> Dict[str, Any]:
    now = now_iso()
    return {
        "id": make_id("stage"),
        "skill_id": skill_id,
        "name": name,
        "description": description,
        "status": STATUS_TODO,
        "created_at": now,
        "updated_at": now,
        "milestone_ids": [],
    }


def new_milestone(stage_id: str, name: str, description: str = "") -> Dict[str, Any]:
    now = now_iso()
    return {
        "id": make_id("milestone"),
        "stage_id": stage_id,
        "name": name,
        "description": description,
        "status": STATUS_TODO,
        "created_at": now,
        "updated_at": now,
        "scope_ids": [],
    }


def new_scope(
    milestone_id: str,
    name: str,
    start_date: str = "",
    end_date: str = "",
) -> Dict[str, Any]:
    now = now_iso()
    return {
        "id": make_id("scope"),
        "milestone_id": milestone_id,
        "name": name,
        "start_date": start_date,
        "end_date": end_date,
        "status": STATUS_TODO,
        "created_at": now,
        "updated_at": now,
        "task_ids": [],
    }


def new_task(
    scope_id: str,
    name: str,
    kind: str = TASK_KIND_TASK,
    weight: int = 1,
) -> Dict[str, Any]:
    now = now_iso()
    return {
        "id": make_id("task"),
        "scope_id": scope_id,
        "name": name,
        "kind": kind,
        "weight": max(1, int(weight)),
        "status": STATUS_TODO,
        "created_at": now,
        "updated_at": now,
        "done_at": None,
    }


def new_insight(
    text: str,
    tags: list[str] | None = None,
    group: str = "",
    summary: str = "",
) -> Dict[str, Any]:
    now = now_iso()
    return {
        "id": make_id("insight"),
        "text": text,
        "summary": summary,
        "tags": list(tags or []),
        "group": group,
        "created_at": now,
        "updated_at": now,
    }
