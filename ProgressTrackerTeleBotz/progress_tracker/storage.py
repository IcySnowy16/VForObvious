from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from .config import get_data_file
from .models import (
    default_reminders,
    default_user_settings,
    ensure_user,
    new_db,
    new_user_data,
    now_iso,
)

logger = logging.getLogger(__name__)


def load_db(path: Optional[Path] = None) -> Dict[str, Any]:
    data_file = path or get_data_file()
    if not data_file.exists():
        return new_db()

    try:
        with open(data_file, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception as exc:
        logger.warning("Failed to load DB, using new: %s", exc)
        return new_db()

    return _normalize_db(data)


def save_db(db: Dict[str, Any], path: Optional[Path] = None) -> None:
    data_file = path or get_data_file()
    data_file.parent.mkdir(parents=True, exist_ok=True)

    db["updated_at"] = now_iso()
    payload = json.dumps(db, indent=2, ensure_ascii=True)

    tmp_file = data_file.with_suffix(data_file.suffix + ".tmp")
    tmp_file.write_text(payload, encoding="utf-8")
    tmp_file.replace(data_file)


def get_user_data(
    db: Dict[str, Any],
    chat_id: str | int,
    user_id: str | int,
) -> Dict[str, Any]:
    return ensure_user(db, chat_id, user_id)


def _normalize_db(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return new_db()

    data.setdefault("version", 1)
    data.setdefault("created_at", now_iso())
    data.setdefault("updated_at", now_iso())
    data.setdefault("chats", {})

    chats = data.get("chats")
    if not isinstance(chats, dict):
        data["chats"] = {}
        return data

    for chat in chats.values():
        if not isinstance(chat, dict):
            continue
        users = chat.get("users")
        if not isinstance(users, dict):
            continue
        for user_id, user in list(users.items()):
            users[user_id] = _normalize_user_data(user)

    return data


def _normalize_user_data(user: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(user, dict):
        return new_user_data()

    user.setdefault("created_at", now_iso())
    user.setdefault("updated_at", now_iso())
    user.setdefault("settings", default_user_settings())
    user.setdefault("reminders", default_reminders())

    for key in ["goals", "skills", "stages", "milestones", "tasks", "insights"]:
        if not isinstance(user.get(key), dict):
            user[key] = {}

    _migrate_scopes_to_tasks(user)
    _normalize_milestones_and_tasks(user)
    return user


def _migrate_scopes_to_tasks(user: Dict[str, Any]) -> None:
    scopes = user.get("scopes")
    if not isinstance(scopes, dict) or not scopes:
        user.pop("scopes", None)
        return

    milestones = user.get("milestones")
    tasks = user.get("tasks")
    if not isinstance(milestones, dict) or not isinstance(tasks, dict):
        user.pop("scopes", None)
        return

    for scope in scopes.values():
        if not isinstance(scope, dict):
            continue
        milestone_id = scope.get("milestone_id")
        if not milestone_id:
            continue
        task_ids = scope.get("task_ids", [])
        if not isinstance(task_ids, list):
            continue
        milestone = milestones.get(milestone_id)
        if isinstance(milestone, dict):
            milestone_task_ids = milestone.setdefault("task_ids", [])
            if not isinstance(milestone_task_ids, list):
                milestone_task_ids = []
                milestone["task_ids"] = milestone_task_ids
            for task_id in task_ids:
                if task_id not in milestone_task_ids:
                    milestone_task_ids.append(task_id)
        for task_id in task_ids:
            task = tasks.get(task_id)
            if isinstance(task, dict):
                task["milestone_id"] = milestone_id
                task.pop("scope_id", None)

    for task_id, task in tasks.items():
        if not isinstance(task, dict):
            continue
        scope_id = task.get("scope_id")
        if not scope_id:
            continue
        scope = scopes.get(scope_id)
        if not isinstance(scope, dict):
            continue
        milestone_id = scope.get("milestone_id")
        if not milestone_id:
            continue
        task["milestone_id"] = milestone_id
        task.pop("scope_id", None)

    user.pop("scopes", None)


def _normalize_milestones_and_tasks(user: Dict[str, Any]) -> None:
    milestones = user.get("milestones")
    tasks = user.get("tasks")
    if not isinstance(milestones, dict) or not isinstance(tasks, dict):
        return

    for milestone in milestones.values():
        if not isinstance(milestone, dict):
            continue
        task_ids = milestone.get("task_ids")
        if not isinstance(task_ids, list):
            milestone["task_ids"] = []
        milestone.pop("scope_ids", None)

    for task_id, task in tasks.items():
        if not isinstance(task, dict):
            continue
        milestone_id = task.get("milestone_id")
        if not milestone_id:
            continue
        milestone = milestones.get(milestone_id)
        if not isinstance(milestone, dict):
            continue
        task_ids = milestone.setdefault("task_ids", [])
        if isinstance(task_ids, list) and task_id not in task_ids:
            task_ids.append(task_id)
