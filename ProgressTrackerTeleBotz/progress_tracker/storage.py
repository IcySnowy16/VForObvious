from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from .config import get_data_file
from .models import ensure_user, new_db, now_iso

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
