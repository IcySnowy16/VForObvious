from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TIMEZONE = "Asia/Singapore"
DEFAULT_DATA_FILE = PROJECT_ROOT / "data" / "progress_data.json"
DEFAULT_BAR_SEGMENTS = 10
DEFAULT_MILESTONES = [50]

PROGRESS_SYMBOLS: Dict[str, str] = {
    "done": "[x]",
    "doing": "[>]",
    "todo": "[ ]",
    "milestone": "[*]",
    "sep": "--",
}

SYMBOL_ENV_KEYS = {
    "done": "SYMBOL_DONE",
    "doing": "SYMBOL_DOING",
    "todo": "SYMBOL_TODO",
    "milestone": "SYMBOL_MILESTONE",
    "sep": "SYMBOL_SEP",
}

EMOJI_ENV_KEYS = {
    "done": "EMOJI_DONE",
    "doing": "EMOJI_DOING",
    "todo": "EMOJI_TODO",
    "milestone": "EMOJI_MILESTONE",
    "sep": "EMOJI_SEP",
}


def load_env() -> None:
    try:
        from dotenv import load_dotenv

        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        env_path = PROJECT_ROOT / ".env"
        if not env_path.exists():
            return
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())


def get_timezone() -> str:
    return os.environ.get("TIMEZONE", DEFAULT_TIMEZONE)


def get_data_file() -> Path:
    raw = os.environ.get("DATA_FILE", str(DEFAULT_DATA_FILE))
    path = Path(raw)
    return path if path.is_absolute() else (PROJECT_ROOT / path)


def get_token() -> str:
    return os.environ.get("TELEGRAM_TOKEN", "")


def get_progress_symbols() -> Dict[str, str]:
    symbols = dict(PROGRESS_SYMBOLS)
    for key, env_key in SYMBOL_ENV_KEYS.items():
        raw = os.environ.get(env_key)
        if raw:
            symbols[key] = raw
    return symbols


def get_emoji_map() -> Dict[str, str]:
    emoji_map: Dict[str, str] = {}
    for key, env_key in EMOJI_ENV_KEYS.items():
        raw = os.environ.get(env_key)
        if raw:
            emoji_map[key] = raw
    return emoji_map


def get_bar_segments() -> int:
    raw = os.environ.get("BAR_SEGMENTS", str(DEFAULT_BAR_SEGMENTS))
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_BAR_SEGMENTS
    return max(1, value)


def get_milestones() -> list[int]:
    raw = os.environ.get("MILESTONES", "")
    if not raw.strip():
        return list(DEFAULT_MILESTONES)

    values: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            value = int(part)
        except ValueError:
            continue
        values.append(max(0, min(100, value)))

    if not values:
        return list(DEFAULT_MILESTONES)

    return sorted(set(values))
