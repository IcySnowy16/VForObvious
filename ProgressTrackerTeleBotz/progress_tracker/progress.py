from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from .config import get_bar_segments, get_emoji_map, get_milestones, get_progress_symbols
from .models import STATUS_DONE

HIERARCHY = {
    "goal": {"store": "goals", "children_key": "skill_ids", "child_type": "skill"},
    "skill": {"store": "skills", "children_key": "stage_ids", "child_type": "stage"},
    "stage": {"store": "stages", "children_key": "milestone_ids", "child_type": "milestone"},
    "milestone": {"store": "milestones", "children_key": "task_ids", "child_type": "task"},
}


@dataclass(frozen=True)
class RenderSettings:
    symbols: Dict[str, str]
    emoji: Dict[str, str]
    milestones: List[int]
    segments: int

    @classmethod
    def from_user_settings(cls, user_settings: Dict[str, Any] | None) -> "RenderSettings":
        symbols = get_progress_symbols()
        emoji_map = get_emoji_map()
        milestones = get_milestones()
        bar_segments = get_bar_segments()

        if user_settings:
            user_symbols = user_settings.get("symbols") or {}
            if isinstance(user_symbols, dict):
                for key, value in user_symbols.items():
                    if value:
                        symbols[str(key)] = str(value)

            user_emoji = user_settings.get("emoji") or {}
            if isinstance(user_emoji, dict):
                for key, value in user_emoji.items():
                    if value:
                        emoji_map[str(key)] = str(value)

            user_milestones = user_settings.get("milestone_positions")
            if isinstance(user_milestones, list) and user_milestones:
                milestones = user_milestones

            user_segments = user_settings.get("bar_segments")
            if isinstance(user_segments, int) and user_segments > 0:
                bar_segments = user_segments

        return cls(
            symbols=symbols,
            emoji=emoji_map,
            milestones=_normalize_milestones(milestones),
            segments=max(1, int(bar_segments)),
        )

    @classmethod
    def from_dict(cls, settings: Dict[str, Any]) -> "RenderSettings":
        base = cls.from_user_settings(None)
        symbols = dict(base.symbols)
        emoji_map = dict(base.emoji)

        raw_symbols = settings.get("symbols") or {}
        if isinstance(raw_symbols, dict):
            symbols.update(raw_symbols)

        raw_emoji = settings.get("emoji") or {}
        if isinstance(raw_emoji, dict):
            emoji_map.update(raw_emoji)

        milestones = settings.get("milestones", base.milestones)
        segments = settings.get("segments", base.segments)

        return cls(
            symbols=symbols,
            emoji=emoji_map,
            milestones=_normalize_milestones(milestones),
            segments=max(1, int(segments)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbols": dict(self.symbols),
            "emoji": dict(self.emoji),
            "milestones": list(self.milestones),
            "segments": int(self.segments),
        }


class ProgressRenderer:
    def __init__(self, settings: RenderSettings) -> None:
        self.settings = settings

    def render_bar(self, percent: float) -> str:
        percent = max(0.0, min(100.0, float(percent)))
        segments = self.settings.segments
        symbols = self.settings.symbols
        emoji_map = self.settings.emoji

        progress_units = percent / 100.0 * segments
        done_segments = int(progress_units)
        has_remainder = progress_units - done_segments > 0
        doing_index = done_segments if has_remainder and done_segments < segments else None

        types: List[str] = []
        for idx in range(segments):
            if idx < done_segments:
                types.append("done")
            elif doing_index == idx:
                types.append("doing")
            else:
                types.append("todo")

        # Milestones are display markers on the bar, not completion percentages.
        for index in _milestone_indices(self.settings.milestones, segments):
            if 0 <= index < segments:
                types[index] = "milestone"

        segments_out = [_resolve_symbol(t, symbols, emoji_map) for t in types]
        sep = _resolve_symbol("sep", symbols, emoji_map)
        return sep.join(segments_out)

    def render_block(self, label: str, percent: float) -> str:
        bar = self.render_bar(percent)
        line = format_progress_line(label, percent)
        return f"{bar}\n{line}"


class ProgressCalculator:
    def __init__(self, user_data: Dict[str, Any]) -> None:
        self.user_data = user_data

    def progress_for(self, entity_type: str, entity_id: str) -> float:
        task_ids = self.collect_task_ids(entity_type, entity_id)
        return self.progress_for_task_ids(task_ids)

    def progress_for_task_ids(self, task_ids: Iterable[str]) -> float:
        tasks = self.user_data.get("tasks", {})
        total_weight = 0
        done_weight = 0

        for task_id in task_ids:
            task = tasks.get(task_id)
            if not isinstance(task, dict):
                continue
            weight = int(task.get("weight", 1))
            total_weight += max(1, weight)
            if task.get("status") == STATUS_DONE:
                done_weight += max(1, weight)

        if total_weight == 0:
            return 0.0

        return done_weight / total_weight * 100.0

    def collect_task_ids(self, entity_type: str, entity_id: str) -> List[str]:
        if entity_type == "task":
            tasks = self.user_data.get("tasks", {})
            return [entity_id] if entity_id in tasks else []

        mapping = HIERARCHY.get(entity_type)
        if not mapping:
            return []

        store = self.user_data.get(mapping["store"], {})
        entity = store.get(entity_id)
        if not isinstance(entity, dict):
            return []

        child_ids = entity.get(mapping["children_key"], [])
        if not isinstance(child_ids, list):
            return []

        child_type = mapping["child_type"]
        if child_type == "task":
            tasks = self.user_data.get("tasks", {})
            return [task_id for task_id in child_ids if task_id in tasks]

        task_ids: List[str] = []
        for child_id in child_ids:
            task_ids.extend(self.collect_task_ids(child_type, child_id))
        return task_ids


def resolve_render_settings(user_settings: Dict[str, Any] | None = None) -> Dict[str, Any]:
    settings = RenderSettings.from_user_settings(user_settings)
    return settings.to_dict()


def render_progress_bar(percent: float, settings: Dict[str, Any]) -> str:
    renderer = ProgressRenderer(_coerce_settings(settings))
    return renderer.render_bar(percent)


def format_progress_line(label: str, percent: float) -> str:
    value = int(round(percent))
    return f"{value}%  {label}"


def render_progress_block(label: str, percent: float, settings: Dict[str, Any]) -> str:
    renderer = ProgressRenderer(_coerce_settings(settings))
    return renderer.render_block(label, percent)


def progress_for_goal(user_data: Dict[str, Any], goal_id: str) -> float:
    calc = ProgressCalculator(user_data)
    return calc.progress_for("goal", goal_id)


def progress_for_skill(user_data: Dict[str, Any], skill_id: str) -> float:
    calc = ProgressCalculator(user_data)
    return calc.progress_for("skill", skill_id)


def progress_for_stage(user_data: Dict[str, Any], stage_id: str) -> float:
    calc = ProgressCalculator(user_data)
    return calc.progress_for("stage", stage_id)


def progress_for_milestone(user_data: Dict[str, Any], milestone_id: str) -> float:
    calc = ProgressCalculator(user_data)
    return calc.progress_for("milestone", milestone_id)


def progress_for_task_ids(user_data: Dict[str, Any], task_ids: Iterable[str]) -> float:
    calc = ProgressCalculator(user_data)
    return calc.progress_for_task_ids(task_ids)


def _coerce_settings(settings: Dict[str, Any] | RenderSettings) -> RenderSettings:
    if isinstance(settings, RenderSettings):
        return settings
    if isinstance(settings, dict):
        return RenderSettings.from_dict(settings)
    return RenderSettings.from_user_settings(None)


def _normalize_milestones(values: Iterable[int]) -> List[int]:
    cleaned: List[int] = []
    for raw in values:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            continue
        cleaned.append(max(0, min(100, value)))
    return sorted(set(cleaned))


def _milestone_indices(milestones: Iterable[int], segments: int) -> List[int]:
    if segments <= 1:
        return [0]
    indices = {
        int(round(m / 100.0 * (segments - 1)))
        for m in milestones
        if isinstance(m, int)
    }
    return sorted(indices)


def _resolve_symbol(kind: str, symbols: Dict[str, str], emoji_map: Dict[str, str]) -> str:
    if kind in emoji_map and emoji_map[kind]:
        return str(emoji_map[kind])
    return str(symbols.get(kind, ""))
