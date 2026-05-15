from __future__ import annotations

from datetime import datetime, timezone
import io
import json
from pathlib import Path
import re
import shutil
from typing import Any, Dict, List, Tuple

from telegram import InputFile, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from .config import get_data_file, get_token, load_env
from .models import (
    STATUS_BLOCKED,
    STATUS_DOING,
    STATUS_DONE,
    STATUS_TODO,
    TASK_KIND_EXPERIMENT,
    TASK_KIND_TASK,
    default_reminders,
    default_user_settings,
    new_goal,
    new_insight,
    new_milestone,
    new_skill,
    new_stage,
    new_task,
    new_user_data,
    touch,
)
from .progress import (
    HIERARCHY,
    progress_for_goal,
    progress_for_milestone,
    progress_for_skill,
    progress_for_stage,
    progress_for_task_ids,
    render_progress_bar,
    render_progress_block,
    resolve_render_settings,
)
from .reminders import apply_user_reminders, schedule_all_reminders
from .storage import get_user_data, load_db, save_db

ALLOWED_SYMBOL_KEYS = {"done", "doing", "todo", "milestone", "sep"}
RESET_WORDS = {"reset", "default", "clear"}
REMOVE_WORDS = {"none", "remove", "off"}
SKIP_WORDS = {"skip", "later"}

INTERNAL_LEVELS = ["goal", "skill", "stage", "milestone", "task"]
DEFAULT_LEVEL_ORDER = ["goal", "skill", "milestone", "task"]
DEFAULT_LEVEL_LABELS = {
    "goal": "goal",
    "skill": "skill",
    "stage": "stage",
    "milestone": "milestone",
    "task": "task",
}
MAX_LEVELS = 10

STORE_BY_TYPE = {
    "goal": "goals",
    "skill": "skills",
    "stage": "stages",
    "milestone": "milestones",
    "task": "tasks",
}

CHILDREN_KEY_BY_TYPE = {
    "goal": "skill_ids",
    "skill": "stage_ids",
    "stage": "milestone_ids",
    "milestone": "task_ids",
}

PARENT_META = {
    "skill": ("goal", "goal_id", "skill_ids"),
    "stage": ("skill", "skill_id", "stage_ids"),
    "milestone": ("stage", "stage_id", "milestone_ids"),
    "task": ("milestone", "milestone_id", "task_ids"),
}

LEVEL_KEY_ALIASES = {
    "goals": "goal",
    "skills": "skill",
    "stages": "stage",
    "milestones": "milestone",
    "tasks": "task",
    "experiment": "task",
    "experiments": "task",
}

STATE_MILESTONE_POSITION = 1
STATE_MILESTONE_EMOJI = 2
STATE_MILESTONE_SYMBOLS = 3
STATE_IMPORT_FILE = 4

IMPORT_MAX_BYTES = 5 * 1024 * 1024
EXPORT_SCHEMA_USER = "progress_tracker_user"

HELP_TEXT = (
    "Commands:\n"
    "/start - Show help\n"
    "/ping - Health check\n"
    "/add_updates <text> - Log update requests\n"
    "/add <level> <name> [| to=<parent> | desc=...]\n"
    "/edit <level> <id|name> | name=... | desc=... | status=... | to=<parent>\n"
    "/delete <level> <id|name> [| cascade=false]\n"
    "/set_levels 4=Goal 3=Skill 2=Milestone 1=Task\n"
    "/view_levels\n"
    "/list <level>\n"
    "/add_goal <name> [| desc] [| milestone1 50%, milestone2 80%]\n"
    "/goal_to_milestones <goal_id|goal_name> | milestone1 50%, milestone2 80%\n"
    "/list_goals\n"
    "/add_skill <goal_id> <name> [| desc]\n"
    "/list_skills\n"
    "/add_stage <skill_id> <name> [| desc]\n"
    "/list_stages\n"
    "/add_milestone <stage_id> <name> [| desc]\n"
    "/list_milestones\n"
    "/add_task <milestone_id> <name> [| kind=task|experiment | weight=2]\n"
    "/list_tasks [milestone_id]\n"
    "/complete_task <task_id>\n"
    "/complete_task <milestone_id> | <task name or #index>\n"
    "/progress <level> <id|name>\n"
    "/add_insight <text> [| tags=a,b | group=foo]\n"
    "/list_insights [all|untagged|unsummarized|pending]\n"
    "/update_insight <insight_id> [| text=... | summary=... | tags=a,b | group=...]\n"
    "/remind daily HH:MM\n"
    "/remind weekly <mon|tue|...|0-6> HH:MM\n"
    "/remind off\n"
    "/remind status\n"
    "/set_milestone - Guided milestone setup\n"
    "/set_milestones <20,50,80|reset> - Quick milestone positions\n"
    "/set_symbols key=value ... - Override bar symbols\n"
    "/set_emoji key=value ... - Override bar emoji\n"
    "/view_settings - Show current render settings\n"
    "/export_data [all] - Export JSON backup\n"
    "/import_data [replace|merge] [all] - Import JSON backup\n\n"
    "Examples:\n"
    "/add_goal Learn Japanese | Long term goal\n"
    "/add_goal Stress Management | Long term goal | Failure Mgmt 50%, Task Manager 80%\n"
    "/goal_to_milestones Stress Management | Failure Mgmt 50%, Task Manager 80%\n"
    "/set_levels 4=Goal 3=Skill 2=Milestone 1=Task\n"
    "/add goal Stress Management\n"
    "/add skill Failure Management | to=Stress Management\n"
    "/add_skill goal_123 Kana Recognition\n"
    "/add_task milestone_123 Drill 10 mins | kind=task | weight=1\n"
    "/list_tasks milestone_123\n"
    "/complete_task milestone_123 | #2\n"
    "/add_insight Daily review felt rushed | tags=process,energy | group=weekly\n"
    "/progress skill skill_123\n"
    "/remind daily 20:00\n"
    "/remind weekly mon 09:00\n"
    "/set_milestone\n"
    "/set_milestones 25,50,75\n"
    "/set_symbols done=[x] doing=[>] milestone=[*]\n"
    "/set_emoji done=OK doing=WORK milestone=STAR\n"
    "/export_data\n"
    "/import_data replace\n"
)

UPDATES_LOG = Path(__file__).resolve().parent.parent / "data" / "updates_log.jsonl"


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(HELP_TEXT)


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    now = datetime.now(timezone.utc).isoformat()
    await update.message.reply_text(f"pong ({now})")


async def cmd_add_updates(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /add_updates <text>")
        return

    text = " ".join(context.args).strip()
    if not text:
        await update.message.reply_text("Please provide update text.")
        return

    entry = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "chat_id": update.effective_chat.id,
        "user_id": update.effective_user.id,
        "user_name": update.effective_user.username or update.effective_user.first_name,
        "text": text,
    }
    _append_update(entry)
    await update.message.reply_text("Saved. I will review and apply changes manually.")


async def cmd_view_levels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _, user = _load_user(update)
    settings = user.get("settings", {})
    config = _get_level_config(settings)
    await update.message.reply_text(_format_level_config(config))


async def cmd_set_levels(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /set_levels 5=Goal 4=Skill 3=Milestone 2=Experiment 1=Task"
        )
        return

    db, user = _load_user(update)
    settings = user.setdefault("settings", {})
    config = _get_level_config(settings)

    level_updates, label_updates, errors = _parse_level_assignments(context.args)
    if errors:
        await update.message.reply_text("Errors: " + "; ".join(errors))
        return

    order = list(config["order"])
    labels = dict(config["labels"])
    warnings: List[str] = []

    for level, label in level_updates.items():
        if level < 1 or level > len(order):
            warnings.append(f"Level {level} is out of range (1-{len(order)}).")
            continue
        internal_type = order[len(order) - level]
        if _is_clear_value(label):
            labels[internal_type] = DEFAULT_LEVEL_LABELS.get(internal_type, internal_type)
        else:
            labels[internal_type] = label

    for internal_type, label in label_updates.items():
        if _is_clear_value(label):
            labels[internal_type] = DEFAULT_LEVEL_LABELS.get(internal_type, internal_type)
        else:
            labels[internal_type] = label

    settings["level_order"] = order
    settings["level_labels"] = labels
    touch(user)
    save_db(db)

    msg = _format_level_config(_get_level_config(settings))
    if warnings:
        msg += "\nWarnings:\n- " + "\n- ".join(warnings)
    if len(order) > MAX_LEVELS:
        msg += f"\nNote: Only the first {MAX_LEVELS} levels are active."
    await update.message.reply_text(msg)


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /add <level> <name> [| to=<parent> | desc=...]"
        )
        return

    raw = " ".join(context.args)
    head, kv = _parse_name_kv(raw)
    level_token, name = _split_level_name(head)
    if not level_token or not name:
        await update.message.reply_text(
            "Usage: /add <level> <name> [| to=<parent> | desc=...]"
        )
        return

    db, user = _load_user(update)
    settings = user.get("settings", {})
    config = _get_level_config(settings)
    entity_type, error = _resolve_level_type(level_token, config)
    if error:
        await update.message.reply_text(error)
        return

    if entity_type == "task":
        level_normalized = _normalize_text(level_token)
        if level_normalized in {"experiment", "experiments"} and "kind" not in kv:
            kv["kind"] = "experiment"

    order = config["order"]
    index = _index_in_order(order, entity_type)
    if index is None:
        await update.message.reply_text("Unknown level.")
        return

    parent_ref = kv.get("to") or kv.get("parent")
    parent_type = None
    parent_id = None

    if index > 0:
        if not parent_ref:
            await update.message.reply_text("Please provide a parent via to=<name|id>.")
            return
        parent_type = order[index - 1]
        parent_id, error = _resolve_entity_id(user, parent_type, parent_ref)
        if error:
            await update.message.reply_text(error)
            return
        parent_id, error = _ensure_parent_for_child(user, parent_type, parent_id, entity_type)
        if error:
            await update.message.reply_text(error)
            return
    elif parent_ref:
        await update.message.reply_text("Top level items do not take a parent.")
        return

    entity_id = _create_entity(user, entity_type, parent_id, name, kv)
    touch(user)
    save_db(db)

    label = _label_for_type(entity_type, config)
    await update.message.reply_text(f"Added {label}: {entity_id} - {name}")


async def cmd_edit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /edit <level> <id|name> | name=... | desc=... | status=... | to=<parent>"
        )
        return

    raw = " ".join(context.args)
    head, kv = _parse_name_kv(raw)
    level_token, selector = _split_level_name(head)
    if not level_token or not selector:
        await update.message.reply_text(
            "Usage: /edit <level> <id|name> | name=... | desc=... | status=... | to=<parent>"
        )
        return

    db, user = _load_user(update)
    settings = user.get("settings", {})
    config = _get_level_config(settings)
    entity_type, error = _resolve_level_type(level_token, config)
    if error:
        await update.message.reply_text(error)
        return

    entity_id, error = _resolve_entity_id(user, entity_type, selector, include_hidden=True)
    if error:
        await update.message.reply_text(error)
        return

    store = user.get(_store_name(entity_type), {})
    payload = store.get(entity_id)
    if not isinstance(payload, dict):
        await update.message.reply_text("Item not found.")
        return

    parent_ref = kv.get("to") or kv.get("parent")
    moved = False
    if parent_ref:
        order = config.get("order", [])
        index = _index_in_order(order, entity_type)
        if index is None:
            await update.message.reply_text("Unknown level.")
            return
        if index == 0:
            await update.message.reply_text("Top level items do not take a parent.")
            return
        if _is_hidden(payload):
            await update.message.reply_text("Auto-created items cannot be moved.")
            return
        parent_type = order[index - 1]
        parent_id, error = _resolve_entity_id(user, parent_type, parent_ref)
        if error:
            await update.message.reply_text(error)
            return
        parent_id, error = _ensure_parent_for_child(user, parent_type, parent_id, entity_type)
        if error:
            await update.message.reply_text(error)
            return
        moved = _move_entity(user, entity_type, entity_id, parent_id)
        if not moved:
            await update.message.reply_text("Item is already under that parent.")
            return

    updated, errors = _apply_edit_updates(entity_type, payload, kv)
    if errors:
        await update.message.reply_text("Errors: " + "; ".join(errors))
        return
    if moved:
        updated.append("parent")
    if not updated:
        await update.message.reply_text("No fields provided to update.")
        return

    touch(payload)
    touch(user)
    save_db(db)
    await update.message.reply_text(f"Updated {entity_id}: {', '.join(updated)}")


async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /delete <level> <id|name> [| cascade=false]")
        return

    raw = " ".join(context.args)
    head, kv = _parse_name_kv(raw)
    level_token, selector = _split_level_name(head)
    if not level_token or not selector:
        await update.message.reply_text("Usage: /delete <level> <id|name> [| cascade=false]")
        return

    db, user = _load_user(update)
    settings = user.get("settings", {})
    config = _get_level_config(settings)
    entity_type, error = _resolve_level_type(level_token, config)
    if error:
        await update.message.reply_text(error)
        return

    entity_id, error = _resolve_entity_id(user, entity_type, selector, include_hidden=True)
    if error:
        await update.message.reply_text(error)
        return

    cascade = _parse_bool(kv.get("cascade", "true"))
    if cascade is None:
        await update.message.reply_text("Invalid cascade value. Use true or false.")
        return

    if not cascade and _has_children(user, entity_type, entity_id):
        await update.message.reply_text("Item has children. Use cascade=true to delete.")
        return

    deleted_count = _delete_entity(user, entity_type, entity_id, cascade)
    touch(user)
    save_db(db)
    await update.message.reply_text(f"Deleted {deleted_count} item(s).")


async def cmd_add_goal(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /add_goal <name> [| desc] [| milestone1 50%, milestone2 80%]"
        )
        return
    name, description, milestones = _parse_goal_with_milestones(" ".join(context.args))
    if not name:
        await update.message.reply_text("Please provide a name.")
        return

    db, user = _load_user(update)
    goal = new_goal(name, description)
    user["goals"][goal["id"]] = goal

    created_milestones = []
    if milestones:
        skill = new_skill(goal["id"], "Milestones", "Auto-created for multi-add")
        user["skills"][skill["id"]] = skill
        goal.setdefault("skill_ids", []).append(skill["id"])

        stage = new_stage(skill["id"], "Milestones", "Auto-created for multi-add")
        user["stages"][stage["id"]] = stage
        skill.setdefault("stage_ids", []).append(stage["id"])

        for item in milestones:
            milestone = new_milestone(stage["id"], item["name"], item.get("description", ""))
            if item.get("target_percent") is not None:
                milestone["target_percent"] = item["target_percent"]
            user["milestones"][milestone["id"]] = milestone
            stage.setdefault("milestone_ids", []).append(milestone["id"])
            created_milestones.append(milestone["id"])

        touch(skill)
        touch(stage)
        touch(goal)

    touch(user)
    save_db(db)
    suffix = f" (milestones: {len(created_milestones)})" if created_milestones else ""
    await update.message.reply_text(f"Goal added: {goal['id']} - {goal['name']}{suffix}")


async def cmd_goal_to_milestones(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /goal_to_milestones <goal_id|goal_name> | milestone1 50%, milestone2 80%"
        )
        return

    raw = " ".join(context.args)
    parts = [part.strip() for part in raw.split("|") if part.strip()]
    if len(parts) < 2:
        await update.message.reply_text(
            "Please provide a goal and at least one milestone after '|'."
        )
        return

    goal_ref = parts[0]
    milestone_raw = " | ".join(parts[1:])
    milestones = _parse_milestones_list(milestone_raw)
    if not milestones:
        await update.message.reply_text("Please provide milestones like Name 50%, Other 80%.")
        return

    db, user = _load_user(update)
    goal_id, error = _resolve_goal_id(user, goal_ref)
    if error:
        await update.message.reply_text(error)
        return

    goal = user.get("goals", {}).get(goal_id)
    if not goal:
        await update.message.reply_text("Goal not found.")
        return

    skill, stage = _ensure_milestone_container(user, goal_id)

    existing_names = set()
    for milestone_id in stage.get("milestone_ids", []):
        milestone = user.get("milestones", {}).get(milestone_id)
        if isinstance(milestone, dict):
            existing_names.add(_normalize_text(milestone.get("name", "")))

    created = 0
    for item in milestones:
        name = item.get("name", "")
        if not name:
            continue
        if _normalize_text(name) in existing_names:
            continue
        milestone = new_milestone(stage["id"], name, item.get("description", ""))
        if item.get("target_percent") is not None:
            milestone["target_percent"] = item["target_percent"]
        user["milestones"][milestone["id"]] = milestone
        stage.setdefault("milestone_ids", []).append(milestone["id"])
        existing_names.add(_normalize_text(name))
        created += 1

    touch(skill)
    touch(stage)
    touch(goal)
    touch(user)
    save_db(db)

    await update.message.reply_text(
        f"Added {created} milestone(s) to goal {goal['name']} ({goal_id})."
    )


async def cmd_list_goals(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _, user = _load_user(update)
    msg = _format_items("Goals", user.get("goals", {}))
    await update.message.reply_text(msg)


async def cmd_add_skill(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /add_skill <goal_id> <name> [| desc]")
        return

    goal_id = context.args[0]
    name, description = _parse_name_desc(" ".join(context.args[1:]))
    if not name:
        await update.message.reply_text("Please provide a name.")
        return

    db, user = _load_user(update)
    goal = user.get("goals", {}).get(goal_id)
    if not goal:
        await update.message.reply_text("Goal not found.")
        return

    skill = new_skill(goal_id, name, description)
    user["skills"][skill["id"]] = skill
    goal.setdefault("skill_ids", []).append(skill["id"])
    touch(goal)
    touch(user)
    save_db(db)
    await update.message.reply_text(f"Skill added: {skill['id']} - {skill['name']}")


async def cmd_list_skills(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _, user = _load_user(update)
    msg = _format_items("Skills", user.get("skills", {}))
    await update.message.reply_text(msg)


async def cmd_add_stage(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /add_stage <skill_id> <name> [| desc]")
        return

    skill_id = context.args[0]
    name, description = _parse_name_desc(" ".join(context.args[1:]))
    if not name:
        await update.message.reply_text("Please provide a name.")
        return

    db, user = _load_user(update)
    skill = user.get("skills", {}).get(skill_id)
    if not skill:
        await update.message.reply_text("Skill not found.")
        return

    stage = new_stage(skill_id, name, description)
    user["stages"][stage["id"]] = stage
    skill.setdefault("stage_ids", []).append(stage["id"])
    touch(skill)
    touch(user)
    save_db(db)
    await update.message.reply_text(f"Stage added: {stage['id']} - {stage['name']}")


async def cmd_list_stages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _, user = _load_user(update)
    msg = _format_items("Stages", user.get("stages", {}))
    await update.message.reply_text(msg)


async def cmd_add_milestone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /add_milestone <stage_id> <name> [| desc]")
        return

    stage_id = context.args[0]
    name, description = _parse_name_desc(" ".join(context.args[1:]))
    if not name:
        await update.message.reply_text("Please provide a name.")
        return

    db, user = _load_user(update)
    stage = user.get("stages", {}).get(stage_id)
    if not stage:
        await update.message.reply_text("Stage not found.")
        return

    milestone = new_milestone(stage_id, name, description)
    user["milestones"][milestone["id"]] = milestone
    stage.setdefault("milestone_ids", []).append(milestone["id"])
    touch(stage)
    touch(user)
    save_db(db)
    await update.message.reply_text(f"Milestone added: {milestone['id']} - {milestone['name']}")


async def cmd_list_milestones(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _, user = _load_user(update)
    msg = _format_milestones_with_progress(user)
    await update.message.reply_text(msg)


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /list <level>")
        return

    level_token = context.args[0]
    _, user = _load_user(update)
    settings = user.get("settings", {})
    config = _get_level_config(settings)
    entity_type, error = _resolve_level_type(level_token, config)
    if error:
        await update.message.reply_text(error)
        return

    if entity_type == "milestone":
        msg = _format_milestones_with_progress(user)
    elif entity_type == "task":
        msg = _format_tasks_grouped(user)
    else:
        title = _label_for_type(entity_type, config).title()
        msg = _format_items(title, user.get(_store_name(entity_type), {}))

    await update.message.reply_text(msg)


async def cmd_add_scope(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Scope level was removed. Use /add_task <milestone_id> <name> instead."
    )


async def cmd_list_scopes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Scope level was removed. Use /list_tasks instead.")


async def cmd_add_task(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /add_task <milestone_id> <name> [| kind=task|experiment | weight=2]"
        )
        return

    milestone_id = context.args[0]
    name, kv = _parse_name_kv(" ".join(context.args[1:]))
    if not name:
        await update.message.reply_text("Please provide a name.")
        return

    db, user = _load_user(update)
    milestone = user.get("milestones", {}).get(milestone_id)
    if not milestone:
        await update.message.reply_text("Milestone not found.")
        return

    kind_raw = (kv.get("kind") or "task").lower()
    kind = TASK_KIND_TASK if kind_raw != "experiment" else TASK_KIND_EXPERIMENT
    weight = _parse_int(kv.get("weight"), default=1)

    task = new_task(milestone_id, name, kind, weight)
    user["tasks"][task["id"]] = task
    milestone.setdefault("task_ids", []).append(task["id"])
    touch(milestone)
    touch(user)
    save_db(db)
    await update.message.reply_text(f"Task added: {task['id']} - {task['name']}")


async def cmd_list_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _, user = _load_user(update)
    if context.args:
        milestone_ref = " ".join(context.args).strip()
        milestone_id, error = _resolve_milestone_id(user, milestone_ref)
        if error:
            await update.message.reply_text(error)
            return
        msg = _format_tasks_for_milestone(user, milestone_id)
    else:
        msg = _format_tasks_grouped(user)
    await update.message.reply_text(msg)


async def cmd_complete_task(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /complete_task <task_id> OR /complete_task <milestone_id> | <task name or #index>"
        )
        return

    raw = " ".join(context.args).strip()
    db, user = _load_user(update)
    task_id, error = _resolve_task_selector(user, raw)
    if error:
        await update.message.reply_text(error)
        return

    task = user.get("tasks", {}).get(task_id)
    if not task:
        await update.message.reply_text("Task not found.")
        return

    if task.get("status") == STATUS_DONE:
        await update.message.reply_text("Task already marked done.")
        return

    task["status"] = STATUS_DONE
    touch(task)
    touch(user)
    save_db(db)

    progress_msg = ""
    milestone_percent = _milestone_progress_for_task(user, task)
    if milestone_percent is not None:
        progress_msg = f" (milestone {int(round(milestone_percent))}%)"

    await update.message.reply_text(f"Completed: {task.get('name', task_id)}{progress_msg}")


async def cmd_add_insight(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /add_insight <text> [| tags=a,b | group=foo]")
        return

    text, kv = _parse_name_kv(" ".join(context.args))
    if not text:
        await update.message.reply_text("Please provide insight text.")
        return

    raw_tags = kv.get("tags") or kv.get("tag") or ""
    tags = _parse_tags(raw_tags)
    group = kv.get("group", "")
    summary = kv.get("summary", "")

    if _normalize_text(group) in RESET_WORDS or _normalize_text(group) in REMOVE_WORDS:
        group = ""
    if _normalize_text(summary) in RESET_WORDS or _normalize_text(summary) in REMOVE_WORDS:
        summary = ""

    db, user = _load_user(update)
    insights = user.setdefault("insights", {})
    insight = new_insight(text, tags=tags, group=group, summary=summary)
    insights[insight["id"]] = insight
    touch(user)
    save_db(db)

    await update.message.reply_text(f"Insight added: {insight['id']}")


async def cmd_list_insights(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _, user = _load_user(update)
    insights = user.get("insights", {})
    if not insights:
        await update.message.reply_text("Insights: (empty)")
        return

    mode = context.args[0].strip().lower() if context.args else "pending"
    allowed = {"all", "untagged", "unsummarized", "pending"}
    if mode not in allowed:
        await update.message.reply_text(
            "Usage: /list_insights [all|untagged|unsummarized|pending]"
        )
        return

    items = _filter_insights(insights, mode)
    if not items:
        await update.message.reply_text(f"Insights ({mode}): (empty)")
        return

    msg = _format_insight_items(items, mode)
    await update.message.reply_text(msg)


async def cmd_update_insight(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /update_insight <insight_id> [| text=... | summary=... | tags=a,b | group=...]"
        )
        return

    insight_id, kv = _parse_name_kv(" ".join(context.args))
    if not insight_id:
        await update.message.reply_text("Please provide an insight id.")
        return

    db, user = _load_user(update)
    insights = user.setdefault("insights", {})
    insight = insights.get(insight_id)
    if not isinstance(insight, dict):
        await update.message.reply_text("Insight not found.")
        return

    updated: list[str] = []

    if "text" in kv:
        insight["text"] = kv["text"]
        updated.append("text")

    if "summary" in kv:
        summary = kv["summary"]
        if _normalize_text(summary) in RESET_WORDS or _normalize_text(summary) in REMOVE_WORDS:
            insight["summary"] = ""
        else:
            insight["summary"] = summary
        updated.append("summary")

    raw_tags = kv.get("tags") or kv.get("tag")
    if raw_tags is not None:
        insight["tags"] = _parse_tags(raw_tags)
        updated.append("tags")

    if "group" in kv:
        group = kv["group"]
        if _normalize_text(group) in RESET_WORDS or _normalize_text(group) in REMOVE_WORDS:
            insight["group"] = ""
        else:
            insight["group"] = group
        updated.append("group")

    if not updated:
        await update.message.reply_text("No fields provided to update.")
        return

    touch(insight)
    touch(user)
    save_db(db)
    await update.message.reply_text(f"Insight updated: {insight_id} ({', '.join(updated)})")


async def cmd_export_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    scope = _parse_import_export_scope(context.args)
    db = load_db()

    if scope == "all":
        payload = db
    else:
        user = get_user_data(db, update.effective_chat.id, update.effective_user.id)
        payload = _wrap_user_export(user)

    raw = json.dumps(payload, indent=2, ensure_ascii=True)
    buffer = io.BytesIO(raw.encode("utf-8"))
    buffer.seek(0)
    filename = _export_filename(scope, update)

    await update.message.reply_document(
        document=InputFile(buffer, filename=filename),
        caption=f"Exported {scope} backup.",
    )


async def cmd_import_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    mode, scope = _parse_import_args(context.args)
    context.user_data["import_setup"] = {"mode": mode, "scope": scope}

    await update.message.reply_text(
        "Send the JSON file to import now. "
        f"Mode: {mode}. Scope: {scope}. Use /cancel to stop."
    )
    return STATE_IMPORT_FILE


async def _receive_import_file(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    setup = context.user_data.get("import_setup")
    if not setup:
        await update.message.reply_text("Use /import_data first.")
        return ConversationHandler.END

    document = update.message.document if update.message else None
    if not document:
        await update.message.reply_text("Please send a JSON file.")
        return STATE_IMPORT_FILE

    if document.file_size and document.file_size > IMPORT_MAX_BYTES:
        await update.message.reply_text("File is too large. Please send a smaller JSON file.")
        return STATE_IMPORT_FILE

    if not _is_json_document(document):
        await update.message.reply_text("Only .json files are supported.")
        return STATE_IMPORT_FILE

    file = await document.get_file()
    buffer = io.BytesIO()
    await file.download_to_memory(out=buffer)
    raw = buffer.getvalue()

    try:
        payload = json.loads(raw.decode("utf-8"))
    except UnicodeDecodeError:
        await update.message.reply_text("File must be UTF-8 encoded JSON.")
        return STATE_IMPORT_FILE
    except json.JSONDecodeError as exc:
        await update.message.reply_text(f"Invalid JSON: {exc}")
        return STATE_IMPORT_FILE

    db = load_db()
    backup_path = _backup_data_file()
    updated_db, message = _apply_import_payload(db, update, payload, setup["mode"], setup["scope"])
    if updated_db is None:
        await update.message.reply_text(message or "Import failed.")
        return STATE_IMPORT_FILE

    save_db(updated_db)
    context.user_data.pop("import_setup", None)

    backup_note = f" Backup: {backup_path.name}." if backup_path else ""
    await update.message.reply_text(f"Import complete. {message or ''}{backup_note}")
    return ConversationHandler.END


async def cmd_progress(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /progress <level> <id|name>")
        return

    level_token = context.args[0]
    selector = " ".join(context.args[1:]).strip()
    if not selector:
        await update.message.reply_text("Usage: /progress <level> <id|name>")
        return

    db, user = _load_user(update)
    settings = resolve_render_settings(user.get("settings", {}))
    config = _get_level_config(user.get("settings", {}))
    entity_type, error = _resolve_level_type(level_token, config)
    if error:
        await update.message.reply_text(error)
        return

    entity_id, error = _resolve_entity_id(user, entity_type, selector)
    if error:
        await update.message.reply_text(error)
        return

    msg = _format_progress_view(user, entity_type, entity_id, settings, config)
    await update.message.reply_text(msg)


async def cmd_remind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db, user = _load_user(update)
    reminders = user.setdefault("reminders", {})

    if not context.args:
        await update.message.reply_text(_format_reminder_status(reminders))
        return

    action = context.args[0].lower()
    if action in {"status"}:
        await update.message.reply_text(_format_reminder_status(reminders))
        return

    if action in {"off", "disable"}:
        reminders.setdefault("daily", {})["enabled"] = False
        reminders.setdefault("weekly", {})["enabled"] = False
        touch(user)
        save_db(db)
        apply_user_reminders(context.application, update.effective_chat.id, update.effective_user.id, reminders)
        await update.message.reply_text("Reminders disabled.")
        return

    if action == "daily":
        if len(context.args) < 2:
            await update.message.reply_text("Usage: /remind daily HH:MM")
            return
        time_str = context.args[1]
        if not _is_time_str(time_str):
            await update.message.reply_text("Invalid time. Use HH:MM (24h).")
            return
        daily = reminders.setdefault("daily", {})
        daily["enabled"] = True
        daily["time"] = time_str
        touch(user)
        save_db(db)
        apply_user_reminders(context.application, update.effective_chat.id, update.effective_user.id, reminders)
        await update.message.reply_text(f"Daily reminders set for {time_str}.")
        return

    if action == "weekly":
        if len(context.args) < 3:
            await update.message.reply_text("Usage: /remind weekly <mon|tue|...|0-6> HH:MM")
            return
        weekday = _parse_weekday(context.args[1])
        if weekday is None:
            await update.message.reply_text("Invalid weekday. Use mon..sun or 0-6.")
            return
        time_str = context.args[2]
        if not _is_time_str(time_str):
            await update.message.reply_text("Invalid time. Use HH:MM (24h).")
            return
        weekly = reminders.setdefault("weekly", {})
        weekly["enabled"] = True
        weekly["weekday"] = weekday
        weekly["time"] = time_str
        touch(user)
        save_db(db)
        apply_user_reminders(context.application, update.effective_chat.id, update.effective_user.id, reminders)
        await update.message.reply_text(f"Weekly reminders set for {context.args[1]} {time_str}.")
        return

    await update.message.reply_text("Unknown remind command. Try /remind status.")


async def cmd_set_milestones(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /set_milestones 20,50,80 or /set_milestones reset")
        return

    raw = " ".join(context.args).strip()
    lowered = raw.lower()

    db = load_db()
    user = get_user_data(db, update.effective_chat.id, update.effective_user.id)
    settings = user.setdefault("settings", {})

    if lowered in RESET_WORDS:
        settings["milestone_positions"] = None
        touch(user)
        save_db(db)
        await update.message.reply_text("Milestones reset to default.")
        return

    values = _parse_percent_list(raw)
    if not values:
        await update.message.reply_text("Please provide milestone percentages like 20,50,80.")
        return

    settings["milestone_positions"] = values
    touch(user)
    save_db(db)
    await update.message.reply_text(f"Milestones set to: {values}")


async def cmd_set_milestone(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if context.args:
        await cmd_set_milestones(update, context)
        return ConversationHandler.END
    await update.message.reply_text(
        "Step 1/3: Enter milestone position (0-100). "
        "You can provide multiple, like 20,50,80."
    )
    return STATE_MILESTONE_POSITION


async def cmd_set_symbols(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_key_value_update(update, context, "symbols")


async def cmd_set_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _handle_key_value_update(update, context, "emoji")


async def cmd_view_settings(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    db = load_db()
    user = get_user_data(db, update.effective_chat.id, update.effective_user.id)
    user_settings = user.get("settings", {})
    settings = resolve_render_settings(user_settings)

    bar = render_progress_bar(45, settings)
    symbols = _format_map(settings.get("symbols", {}))
    emoji_map = _format_map(settings.get("emoji", {}))
    milestones = settings.get("milestones", [])
    segments = settings.get("segments")

    msg = (
        "Current render settings:\n"
        f"segments: {segments}\n"
        f"milestones: {milestones}\n"
        f"symbols: {symbols}\n"
        f"emoji: {emoji_map}\n\n"
        "Preview (45%):\n"
        f"{bar}"
    )
    await update.message.reply_text(msg)


async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("milestone_setup", None)
    context.user_data.pop("import_setup", None)
    await update.message.reply_text("Operation cancelled.")
    return ConversationHandler.END


def build_application(token: str | None = None) -> Application:
    load_env()
    token = token or get_token()
    if not token:
        raise ValueError("TELEGRAM_TOKEN is not set")

    app = Application.builder().token(token).build()

    milestone_flow = ConversationHandler(
        entry_points=[
            CommandHandler("set_milestone", cmd_set_milestone),
            CommandHandler("set_milestones", cmd_set_milestone),
        ],
        states={
            STATE_MILESTONE_POSITION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _milestone_position),
            ],
            STATE_MILESTONE_EMOJI: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _milestone_emoji),
            ],
            STATE_MILESTONE_SYMBOLS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _milestone_symbols),
            ],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    )

    import_flow = ConversationHandler(
        entry_points=[CommandHandler("import_data", cmd_import_data)],
        states={
            STATE_IMPORT_FILE: [
                MessageHandler(filters.Document.ALL, _receive_import_file),
            ],
        },
        fallbacks=[CommandHandler("cancel", cmd_cancel)],
    )

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("add_updates", cmd_add_updates))
    app.add_handler(CommandHandler("view_levels", cmd_view_levels))
    app.add_handler(CommandHandler("set_levels", cmd_set_levels))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("edit", cmd_edit))
    app.add_handler(CommandHandler("delete", cmd_delete))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("add_goal", cmd_add_goal))
    app.add_handler(CommandHandler("goal_to_milestones", cmd_goal_to_milestones))
    app.add_handler(CommandHandler("list_goals", cmd_list_goals))
    app.add_handler(CommandHandler("add_skill", cmd_add_skill))
    app.add_handler(CommandHandler("list_skills", cmd_list_skills))
    app.add_handler(CommandHandler("add_stage", cmd_add_stage))
    app.add_handler(CommandHandler("list_stages", cmd_list_stages))
    app.add_handler(CommandHandler("add_milestone", cmd_add_milestone))
    app.add_handler(CommandHandler("list_milestones", cmd_list_milestones))
    app.add_handler(CommandHandler("add_scope", cmd_add_scope))
    app.add_handler(CommandHandler("list_scopes", cmd_list_scopes))
    app.add_handler(CommandHandler("add_task", cmd_add_task))
    app.add_handler(CommandHandler("list_tasks", cmd_list_tasks))
    app.add_handler(CommandHandler("complete_task", cmd_complete_task))
    app.add_handler(CommandHandler("progress", cmd_progress))
    app.add_handler(CommandHandler("add_insight", cmd_add_insight))
    app.add_handler(CommandHandler("list_insights", cmd_list_insights))
    app.add_handler(CommandHandler("update_insight", cmd_update_insight))
    app.add_handler(CommandHandler("export_data", cmd_export_data))
    app.add_handler(CommandHandler("remind", cmd_remind))
    app.add_handler(milestone_flow)
    app.add_handler(import_flow)
    app.add_handler(CommandHandler("set_symbols", cmd_set_symbols))
    app.add_handler(CommandHandler("set_emoji", cmd_set_emoji))
    app.add_handler(CommandHandler("view_settings", cmd_view_settings))
    schedule_all_reminders(app)
    return app


def run_polling(token: str | None = None) -> None:
    app = build_application(token)
    app.run_polling()


async def _handle_key_value_update(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    bucket: str,
) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /set_symbols key=value ... or /set_emoji key=value ..."
        )
        return

    updates, errors = _parse_key_value_args(context.args)
    if errors:
        await update.message.reply_text("Errors: " + "; ".join(errors))
        return

    db = load_db()
    user = get_user_data(db, update.effective_chat.id, update.effective_user.id)
    settings = user.setdefault("settings", {})
    store = settings.setdefault(bucket, {})

    for key, value in updates.items():
        if value is None:
            store.pop(key, None)
        else:
            store[key] = value

    touch(user)
    save_db(db)
    await update.message.reply_text(f"Updated {bucket}: {updates}")


async def _milestone_position(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    lowered = text.lower()

    if lowered in RESET_WORDS:
        db = load_db()
        user = get_user_data(db, update.effective_chat.id, update.effective_user.id)
        settings = user.setdefault("settings", {})
        settings["milestone_positions"] = None
        emoji_map = settings.get("emoji", {})
        if isinstance(emoji_map, dict):
            emoji_map.pop("milestone", None)
        touch(user)
        save_db(db)
        await update.message.reply_text("Milestones reset to default.")
        return ConversationHandler.END

    values = _parse_percent_list(text)
    if not values:
        await update.message.reply_text("Please provide numbers like 20,50,80.")
        return STATE_MILESTONE_POSITION

    setup = context.user_data.setdefault("milestone_setup", {})
    setup["positions"] = values

    await update.message.reply_text(
        "Step 2/3: Send the emoji or symbol for the milestone marker. "
        "Type 'skip' to keep current or 'remove' to clear."
    )
    return STATE_MILESTONE_EMOJI


async def _milestone_emoji(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    lowered = text.lower()

    setup = context.user_data.setdefault("milestone_setup", {})
    if lowered in SKIP_WORDS:
        setup["milestone_emoji"] = None
        setup["milestone_emoji_clear"] = False
    elif lowered in REMOVE_WORDS or lowered in RESET_WORDS:
        setup["milestone_emoji"] = None
        setup["milestone_emoji_clear"] = True
    else:
        setup["milestone_emoji"] = text
        setup["milestone_emoji_clear"] = False

    await update.message.reply_text(
        "Step 3/3 (optional): Set default symbols. "
        "Example: doing=[>] done=OK. Type 'skip' to keep current."
    )
    return STATE_MILESTONE_SYMBOLS


async def _milestone_symbols(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    lowered = text.lower()
    setup = context.user_data.setdefault("milestone_setup", {})

    updates: Dict[str, Any] = {}
    if lowered not in SKIP_WORDS:
        args = text.split()
        updates, errors = _parse_key_value_args(args, allowed_keys={"doing", "done"})
        if errors:
            await update.message.reply_text("Errors: " + "; ".join(errors))
            return STATE_MILESTONE_SYMBOLS
    setup["symbol_updates"] = updates

    db = load_db()
    user = get_user_data(db, update.effective_chat.id, update.effective_user.id)
    settings = user.setdefault("settings", {})

    positions = setup.get("positions")
    if positions:
        settings["milestone_positions"] = positions

    emoji_map = settings.setdefault("emoji", {})
    if setup.get("milestone_emoji_clear"):
        emoji_map.pop("milestone", None)
    elif setup.get("milestone_emoji"):
        emoji_map["milestone"] = setup["milestone_emoji"]

    symbols = settings.setdefault("symbols", {})
    for key, value in (setup.get("symbol_updates") or {}).items():
        if value is None:
            symbols.pop(key, None)
        else:
            symbols[key] = value

    touch(user)
    save_db(db)
    context.user_data.pop("milestone_setup", None)

    resolved = resolve_render_settings(settings)
    preview = render_progress_bar(45, resolved)
    await update.message.reply_text("Saved. Preview (45%):\n" + preview)
    return ConversationHandler.END


def _parse_percent_list(raw: str) -> List[int]:
    parts = [p for p in raw.replace(",", " ").split() if p]
    values: List[int] = []
    for part in parts:
        try:
            value = int(part)
        except ValueError:
            continue
        values.append(max(0, min(100, value)))
    return sorted(set(values))


def _parse_key_value_args(
    args: List[str],
    allowed_keys: set[str] | None = None,
) -> Tuple[Dict[str, Any], List[str]]:
    updates: Dict[str, Any] = {}
    errors: List[str] = []
    allowed = allowed_keys or ALLOWED_SYMBOL_KEYS

    for arg in args:
        if "=" not in arg:
            errors.append(f"Missing '=' in {arg}")
            continue
        key, _, value = arg.partition("=")
        key = key.strip().lower()
        value = value.strip()

        if key not in allowed:
            errors.append(f"Unknown key {key}")
            continue
        if not value:
            errors.append(f"Empty value for {key}")
            continue

        lowered = value.lower()
        if lowered in RESET_WORDS or lowered in REMOVE_WORDS:
            updates[key] = None
        else:
            updates[key] = value

    return updates, errors


def _format_map(values: Dict[str, Any]) -> str:
    if not values:
        return "(empty)"
    parts = [f"{key}={values[key]}" for key in sorted(values.keys())]
    return ", ".join(parts)


def _append_update(entry: Dict[str, Any]) -> None:
    UPDATES_LOG.parent.mkdir(parents=True, exist_ok=True)
    line = (
        "{"
        f"\"timestamp\":\"{entry['timestamp']}\","
        f"\"chat_id\":{entry['chat_id']},"
        f"\"user_id\":{entry['user_id']},"
        f"\"user_name\":\"{entry['user_name']}\","
        f"\"text\":\"{_escape_json(entry['text'])}\""
        "}\n"
    )
    UPDATES_LOG.write_text(UPDATES_LOG.read_text(encoding="utf-8") + line if UPDATES_LOG.exists() else line, encoding="utf-8")


def _escape_json(text: str) -> str:
    return text.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ")


def _is_hidden(payload: Any) -> bool:
    return isinstance(payload, dict) and bool(payload.get("hidden"))


def _store_name(entity_type: str) -> str:
    return STORE_BY_TYPE.get(entity_type, "")


def _children_key(entity_type: str) -> str:
    return CHILDREN_KEY_BY_TYPE.get(entity_type, "")


def _normalize_level_order(order: Any) -> List[str]:
    if not isinstance(order, list):
        order = list(DEFAULT_LEVEL_ORDER)
    else:
        has_task = "task" in order
        remapped: List[str] = []
        for item in order:
            if item == "scope" and not has_task:
                remapped.append("task")
            else:
                remapped.append(item)
        order = remapped
    cleaned = [item for item in order if item in INTERNAL_LEVELS]
    ordered = [item for item in INTERNAL_LEVELS if item in cleaned]
    return ordered or list(DEFAULT_LEVEL_ORDER)


def _get_level_config(settings: Dict[str, Any]) -> Dict[str, Any]:
    order = _normalize_level_order(settings.get("level_order"))
    labels = dict(DEFAULT_LEVEL_LABELS)
    stored_labels = settings.get("level_labels")
    if isinstance(stored_labels, dict):
        for key, value in stored_labels.items():
            if key in INTERNAL_LEVELS and isinstance(value, str) and value.strip():
                labels[key] = value.strip()
        legacy_scope = stored_labels.get("scope")
        if legacy_scope and "task" not in stored_labels:
            labels["task"] = str(legacy_scope).strip()

    label_to_type: Dict[str, str] = {}
    for type_name, label in labels.items():
        if label:
            label_to_type[_normalize_text(label)] = type_name
    for type_name in INTERNAL_LEVELS:
        label_to_type[_normalize_text(type_name)] = type_name
    for alias, internal in LEVEL_KEY_ALIASES.items():
        label_to_type[_normalize_text(alias)] = internal

    return {"order": order, "labels": labels, "label_to_type": label_to_type}


def _label_for_type(entity_type: str, config: Dict[str, Any]) -> str:
    labels = config.get("labels", {})
    label = labels.get(entity_type)
    return label if isinstance(label, str) and label.strip() else entity_type


def _format_level_config(config: Dict[str, Any]) -> str:
    order = config.get("order", [])
    labels = config.get("labels", {})
    if not order:
        return "Levels: (empty)"
    total = len(order)
    lines = ["Levels (top -> bottom):"]
    for idx, internal_type in enumerate(order):
        level_num = total - idx
        label = labels.get(internal_type, internal_type)
        suffix = f" (type={internal_type})" if label != internal_type else ""
        lines.append(f"L{level_num}: {label}{suffix}")
    return "\n".join(lines)


def _parse_level_assignments(args: List[str]) -> Tuple[Dict[int, str], Dict[str, str], List[str]]:
    level_updates: Dict[int, str] = {}
    label_updates: Dict[str, str] = {}
    errors: List[str] = []

    for arg in args:
        if "=" not in arg:
            errors.append(f"Missing '=' in {arg}")
            continue
        key, _, value = arg.partition("=")
        key = key.strip()
        value = value.strip()
        if not value:
            errors.append(f"Empty value for {key}")
            continue

        if key.isdigit():
            level_updates[int(key)] = value
            continue

        normalized = _normalize_text(key)
        internal = LEVEL_KEY_ALIASES.get(normalized, normalized)
        if internal in INTERNAL_LEVELS:
            label_updates[internal] = value
        else:
            errors.append(f"Unknown level key {key}")

    return level_updates, label_updates, errors


def _resolve_level_type(token: str, config: Dict[str, Any]) -> Tuple[str | None, str | None]:
    raw = _normalize_text(token)
    if raw.startswith("lvl") and raw[3:].isdigit():
        raw = raw[3:]

    if raw.isdigit():
        order = config.get("order", [])
        level = int(raw)
        if level < 1 or level > len(order):
            return None, f"Level {level} is out of range (1-{len(order)})."
        return order[len(order) - level], None

    label_to_type = config.get("label_to_type", {})
    if raw in label_to_type:
        return label_to_type[raw], None

    return None, f"Unknown level '{token}'. Use /view_levels to see configured levels."


def _index_in_order(order: List[str], entity_type: str) -> int | None:
    try:
        return order.index(entity_type)
    except ValueError:
        return None


def _split_level_name(raw: str) -> Tuple[str, str]:
    parts = raw.strip().split(None, 1)
    if len(parts) < 2:
        return "", ""
    return parts[0], parts[1].strip()


def _parse_bool(raw: str | None) -> bool | None:
    if raw is None:
        return None
    value = _normalize_text(str(raw))
    if value in {"true", "yes", "1", "on"}:
        return True
    if value in {"false", "no", "0", "off"}:
        return False
    return None


def _resolve_entity_id(
    user: Dict[str, Any],
    entity_type: str,
    raw: str,
    include_hidden: bool = False,
) -> Tuple[str | None, str | None]:
    store_name = _store_name(entity_type)
    store = user.get(store_name, {})
    if raw in store:
        return raw, None

    normalized = _normalize_text(raw)
    matches = [
        entity_id
        for entity_id, payload in store.items()
        if isinstance(payload, dict)
        and (include_hidden or not _is_hidden(payload))
        and _normalize_text(payload.get("name", "")) == normalized
    ]
    if len(matches) == 1:
        return matches[0], None
    if not matches:
        return None, "Item not found."
    return None, "Multiple items match that name. Use the id."


def _apply_edit_updates(
    entity_type: str,
    payload: Dict[str, Any],
    kv: Dict[str, str],
) -> Tuple[List[str], List[str]]:
    updated: List[str] = []
    errors: List[str] = []
    pending: Dict[str, Any] = {}

    name = kv.get("name")
    if name is not None:
        pending["name"] = name
        updated.append("name")

    desc = kv.get("desc")
    if desc is None:
        desc = kv.get("description")
    if desc is not None and entity_type in {"goal", "skill", "stage", "milestone"}:
        if _is_clear_value(desc):
            pending["description"] = ""
        else:
            pending["description"] = desc
        updated.append("description")

    status = kv.get("status")
    if status is not None:
        normalized = _normalize_text(status)
        if normalized not in {STATUS_TODO, STATUS_DOING, STATUS_DONE, STATUS_BLOCKED}:
            errors.append("Invalid status (use todo|doing|done|blocked)")
        else:
            pending["status"] = normalized
            updated.append("status")

    if entity_type == "task":
        kind = kv.get("kind")
        if kind is not None:
            normalized = _normalize_text(kind)
            if normalized not in {"task", "experiment"}:
                errors.append("Invalid kind (task|experiment)")
            else:
                pending["kind"] = TASK_KIND_EXPERIMENT if normalized == "experiment" else TASK_KIND_TASK
                updated.append("kind")
        weight = kv.get("weight")
        if weight is not None:
            try:
                parsed = int(weight)
            except ValueError:
                errors.append("Invalid weight (integer)")
            else:
                pending["weight"] = max(1, parsed)
                updated.append("weight")

    if entity_type == "milestone":
        target = kv.get("target") or kv.get("target_percent") or kv.get("percent")
        if target is not None:
            if _is_clear_value(target):
                pending["target_percent"] = None
                updated.append("target_percent")
            else:
                try:
                    parsed = int(target)
                except ValueError:
                    errors.append("Invalid target percent")
                else:
                    pending["target_percent"] = max(0, min(100, parsed))
                    updated.append("target_percent")

    if errors:
        return [], errors

    for key, value in pending.items():
        payload[key] = value

    return updated, errors


def _is_clear_value(value: str) -> bool:
    return _normalize_text(value) in RESET_WORDS or _normalize_text(value) in REMOVE_WORDS


def _has_children(user: Dict[str, Any], entity_type: str, entity_id: str) -> bool:
    key = _children_key(entity_type)
    if not key:
        return False
    store = user.get(_store_name(entity_type), {})
    payload = store.get(entity_id)
    if not isinstance(payload, dict):
        return False
    child_ids = payload.get(key, [])
    return isinstance(child_ids, list) and bool(child_ids)


def _delete_entity(
    user: Dict[str, Any],
    entity_type: str,
    entity_id: str,
    cascade: bool,
) -> int:
    deleted = 0
    store = user.get(_store_name(entity_type), {})
    payload = store.get(entity_id)
    if not isinstance(payload, dict):
        return 0

    if cascade:
        for child_type, child_id in _collect_descendants(user, entity_type, entity_id):
            child_store = user.get(_store_name(child_type), {})
            if child_id in child_store:
                del child_store[child_id]
                deleted += 1

    _detach_from_parent(user, entity_type, entity_id)
    if entity_id in store:
        del store[entity_id]
        deleted += 1
    return deleted


def _collect_descendants(
    user: Dict[str, Any],
    entity_type: str,
    entity_id: str,
) -> List[Tuple[str, str]]:
    mapping = HIERARCHY.get(entity_type)
    if not mapping:
        return []

    store = user.get(mapping["store"], {})
    payload = store.get(entity_id)
    if not isinstance(payload, dict):
        return []

    child_ids = payload.get(mapping["children_key"], [])
    if not isinstance(child_ids, list):
        return []

    child_type = mapping["child_type"]
    results: List[Tuple[str, str]] = []
    for child_id in child_ids:
        results.extend(_collect_descendants(user, child_type, child_id))
        results.append((child_type, child_id))
    return results


def _detach_from_parent(user: Dict[str, Any], entity_type: str, entity_id: str) -> None:
    meta = PARENT_META.get(entity_type)
    if not meta:
        return
    parent_type, parent_id_key, child_ids_key = meta
    store = user.get(_store_name(entity_type), {})
    payload = store.get(entity_id)
    if not isinstance(payload, dict):
        return
    parent_id = payload.get(parent_id_key)
    if not parent_id:
        return
    parent_store = user.get(_store_name(parent_type), {})
    parent = parent_store.get(parent_id)
    if not isinstance(parent, dict):
        return
    child_ids = parent.get(child_ids_key)
    if isinstance(child_ids, list) and entity_id in child_ids:
        child_ids.remove(entity_id)


def _move_entity(
    user: Dict[str, Any],
    entity_type: str,
    entity_id: str,
    new_parent_id: str,
) -> bool:
    meta = PARENT_META.get(entity_type)
    if not meta:
        return False

    parent_type, parent_id_key, child_ids_key = meta
    store = user.get(_store_name(entity_type), {})
    payload = store.get(entity_id)
    if not isinstance(payload, dict):
        return False

    old_parent_id = payload.get(parent_id_key)
    if old_parent_id == new_parent_id:
        return False

    _detach_from_parent(user, entity_type, entity_id)
    payload[parent_id_key] = new_parent_id

    parent_store = user.get(_store_name(parent_type), {})
    parent = parent_store.get(new_parent_id)
    if isinstance(parent, dict):
        child_ids = parent.setdefault(child_ids_key, [])
        if isinstance(child_ids, list) and entity_id not in child_ids:
            child_ids.append(entity_id)
        touch(parent)

    return True


def _ensure_parent_for_child(
    user: Dict[str, Any],
    parent_type: str,
    parent_id: str,
    child_type: str,
) -> Tuple[str | None, str | None]:
    parent_meta = PARENT_META.get(child_type)
    if not parent_meta:
        return parent_id, None
    expected_parent = parent_meta[0]
    if parent_type == expected_parent:
        return parent_id, None

    if parent_type not in INTERNAL_LEVELS or expected_parent not in INTERNAL_LEVELS:
        return None, "Invalid parent chain."

    start_idx = INTERNAL_LEVELS.index(parent_type)
    end_idx = INTERNAL_LEVELS.index(expected_parent)
    if start_idx > end_idx:
        return None, "Parent level must be above the child level."

    current_type = parent_type
    current_id = parent_id
    for next_type in INTERNAL_LEVELS[start_idx + 1 : end_idx + 1]:
        current_id = _get_or_create_auto_child(user, current_type, current_id, next_type)
        current_type = next_type

    return current_id, None


def _get_or_create_auto_child(
    user: Dict[str, Any],
    parent_type: str,
    parent_id: str,
    child_type: str,
) -> str:
    parent_store = user.get(_store_name(parent_type), {})
    parent = parent_store.get(parent_id)
    if not isinstance(parent, dict):
        raise ValueError("Parent not found")

    child_store = user.get(_store_name(child_type), {})
    child_ids = parent.get(_children_key(parent_type), [])
    if isinstance(child_ids, list):
        for child_id in child_ids:
            child = child_store.get(child_id)
            if not isinstance(child, dict):
                continue
            if (
                child.get("auto")
                and child.get("auto_parent_id") == parent_id
                and child.get("auto_child_type") == child_type
            ):
                return child_id

    return _create_entity(
        user,
        child_type,
        parent_id,
        f"__auto__{child_type}",
        {},
        parent_type=parent_type,
        is_auto=True,
    )


def _create_entity(
    user: Dict[str, Any],
    entity_type: str,
    parent_id: str | None,
    name: str,
    kv: Dict[str, str],
    parent_type: str | None = None,
    is_auto: bool = False,
) -> str:
    description = kv.get("desc") or kv.get("description") or ""
    if is_auto:
        name = f"__auto__{entity_type}"
        description = "Auto-created for level mapping."

    store = user.setdefault(_store_name(entity_type), {})
    item: Dict[str, Any]

    if entity_type == "goal":
        item = new_goal(name, description)
    elif entity_type == "skill":
        item = new_skill(parent_id or "", name, description)
    elif entity_type == "stage":
        item = new_stage(parent_id or "", name, description)
    elif entity_type == "milestone":
        item = new_milestone(parent_id or "", name, description)
        target = kv.get("target") or kv.get("target_percent") or kv.get("percent")
        if target and not _is_clear_value(target):
            try:
                parsed = int(target)
            except ValueError:
                parsed = None
            if parsed is not None:
                item["target_percent"] = max(0, min(100, parsed))
    elif entity_type == "task":
        kind_raw = _normalize_text(kv.get("kind", "task"))
        kind = TASK_KIND_EXPERIMENT if kind_raw == "experiment" else TASK_KIND_TASK
        weight = _parse_int(kv.get("weight"), default=1)
        item = new_task(parent_id or "", name, kind, weight)
    else:
        raise ValueError("Unknown entity type")

    if is_auto:
        item["hidden"] = True
        item["auto"] = True
        item["auto_parent_id"] = parent_id
        if parent_type:
            item["auto_parent_type"] = parent_type
        item["auto_child_type"] = entity_type

    store[item["id"]] = item

    if parent_id and not parent_type and entity_type in PARENT_META:
        parent_type = PARENT_META[entity_type][0]

    if parent_id and parent_type:
        parent_store = user.get(_store_name(parent_type), {})
        parent = parent_store.get(parent_id)
        if isinstance(parent, dict):
            child_ids = parent.setdefault(_children_key(parent_type), [])
            if isinstance(child_ids, list):
                child_ids.append(item["id"])
            touch(parent)

    return item["id"]


def _format_progress_view(
    user: Dict[str, Any],
    entity_type: str,
    entity_id: str,
    settings: Dict[str, Any],
    config: Dict[str, Any],
) -> str:
    label = _item_label(user, entity_type, entity_id, config)
    percent = _progress_for_entity(user, entity_type, entity_id)
    block = render_progress_block(label, percent or 0.0, settings)

    order = config.get("order", [])
    idx = _index_in_order(order, entity_type)
    if idx is None or idx >= len(order) - 1:
        return block

    child_type = order[idx + 1]
    child_ids = _collect_child_ids(user, entity_type, entity_id, child_type)
    if not child_ids:
        return block + f"\n\nNo {_label_for_type(child_type, config)} items yet."

    lines = [block, f"{_label_for_type(child_type, config).title()}:"]
    for child_id in child_ids:
        child_label = _item_label(user, child_type, child_id, config)
        child_percent = _progress_for_entity(user, child_type, child_id)
        lines.append(render_progress_block(child_label, child_percent or 0.0, settings))
    return "\n\n".join(lines)


def _collect_child_ids(
    user: Dict[str, Any],
    start_type: str,
    start_id: str,
    target_type: str,
) -> List[str]:
    mapping = HIERARCHY.get(start_type)
    if not mapping:
        return []

    store = user.get(mapping["store"], {})
    payload = store.get(start_id)
    if not isinstance(payload, dict):
        return []

    child_ids = payload.get(mapping["children_key"], [])
    if not isinstance(child_ids, list):
        return []

    child_type = mapping["child_type"]
    results: List[str] = []
    for child_id in child_ids:
        child_store = user.get(_store_name(child_type), {})
        child = child_store.get(child_id)
        if not isinstance(child, dict):
            continue
        if child_type == target_type:
            if _is_hidden(child):
                continue
            results.append(child_id)
        else:
            results.extend(_collect_child_ids(user, child_type, child_id, target_type))
    return results


def _item_label(
    user: Dict[str, Any],
    entity_type: str,
    entity_id: str,
    config: Dict[str, Any],
) -> str:
    store = user.get(_store_name(entity_type), {})
    payload = store.get(entity_id, {})
    name = payload.get("name", entity_id) if isinstance(payload, dict) else entity_id
    label = _label_for_type(entity_type, config).title()
    return f"{label}: {name}"


def _parse_import_export_scope(args: List[str]) -> str:
    for arg in args:
        if arg.strip().lower() in {"all", "db", "full"}:
            return "all"
    return "user"


def _parse_import_args(args: List[str]) -> Tuple[str, str]:
    mode = "replace"
    scope = "user"
    for arg in args:
        lowered = arg.strip().lower()
        if lowered in {"merge", "replace"}:
            mode = lowered
        elif lowered in {"all", "db", "full"}:
            scope = "all"
    return mode, scope


def _wrap_user_export(user: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "schema": EXPORT_SCHEMA_USER,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "user": user,
    }


def _export_filename(scope: str, update: Update) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    if scope == "all":
        return f"progress_db_{stamp}.json"
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    return f"progress_user_{chat_id}_{user_id}_{stamp}.json"


def _is_json_document(document: Any) -> bool:
    name = (getattr(document, "file_name", "") or "").lower()
    mime = (getattr(document, "mime_type", "") or "").lower()
    return name.endswith(".json") or mime == "application/json"


def _backup_data_file() -> Path | None:
    data_file = get_data_file()
    if not data_file.exists():
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup = data_file.with_name(f"{data_file.stem}.backup.{stamp}{data_file.suffix}")
    try:
        shutil.copy2(data_file, backup)
    except OSError:
        return None
    return backup


def _apply_import_payload(
    db: Dict[str, Any],
    update: Update,
    payload: Any,
    mode: str,
    scope: str,
) -> Tuple[Dict[str, Any] | None, str | None]:
    if scope == "all":
        incoming = _extract_db_payload(payload)
        if incoming is None:
            return None, "Expected full database JSON (with 'chats')."
        incoming_db = _normalize_db_payload(incoming)
        if mode == "replace":
            return incoming_db, "Replaced full database."
        return _merge_db_payload(db, incoming_db), "Merged into full database."

    user_payload = _extract_user_payload(payload, update)
    if not isinstance(user_payload, dict):
        return None, "Expected user JSON data."

    chat_key = str(update.effective_chat.id)
    user_key = str(update.effective_user.id)
    chat = db.setdefault("chats", {}).setdefault(chat_key, {"users": {}})
    users = chat.setdefault("users", {})

    if mode == "replace":
        users[user_key] = _normalize_user_payload(user_payload)
        return db, "Replaced user data."

    existing = users.get(user_key, {})
    users[user_key] = _merge_user_payload(existing, user_payload)
    return db, "Merged user data."


def _extract_db_payload(payload: Any) -> Dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if "chats" in payload:
        return payload
    return None


def _extract_user_payload(payload: Any, update: Update) -> Dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if payload.get("schema") == EXPORT_SCHEMA_USER:
        return payload.get("user") if isinstance(payload.get("user"), dict) else None
    if "user" in payload and isinstance(payload.get("user"), dict):
        return payload.get("user")
    if "chats" in payload:
        chat = payload.get("chats", {}).get(str(update.effective_chat.id))
        if isinstance(chat, dict):
            users = chat.get("users", {})
            if isinstance(users, dict):
                return users.get(str(update.effective_user.id))
        return None
    return payload


def _normalize_user_payload(payload: Any, force_updated: bool = True) -> Dict[str, Any]:
    base = new_user_data()
    incoming = payload if isinstance(payload, dict) else {}
    for key, value in incoming.items():
        base[key] = value

    settings = base.get("settings")
    if not isinstance(settings, dict):
        settings = {}
    normalized_settings = default_user_settings()
    normalized_settings.update(settings)
    if not isinstance(normalized_settings.get("symbols"), dict):
        normalized_settings["symbols"] = {}
    if not isinstance(normalized_settings.get("emoji"), dict):
        normalized_settings["emoji"] = {}
    base["settings"] = normalized_settings

    reminders = base.get("reminders")
    normalized_reminders = _merge_reminders(default_reminders(), reminders)
    base["reminders"] = normalized_reminders

    for key in ["goals", "skills", "stages", "milestones", "tasks", "insights"]:
        if not isinstance(base.get(key), dict):
            base[key] = {}

    _migrate_scopes_payload(base)

    if force_updated:
        base["updated_at"] = datetime.now(timezone.utc).isoformat()
    else:
        base.setdefault("updated_at", datetime.now(timezone.utc).isoformat())
    base.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    return base


def _merge_user_payload(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    base = _normalize_user_payload(existing, force_updated=False)
    inc = _normalize_user_payload(incoming, force_updated=False)

    merged = dict(base)
    merged_settings = dict(base.get("settings", {}))
    inc_settings = inc.get("settings", {})
    merged_settings.update(inc_settings)
    merged_settings["symbols"] = {
        **base.get("settings", {}).get("symbols", {}),
        **inc_settings.get("symbols", {}),
    }
    merged_settings["emoji"] = {
        **base.get("settings", {}).get("emoji", {}),
        **inc_settings.get("emoji", {}),
    }
    merged["settings"] = merged_settings

    merged["reminders"] = _merge_reminders(base.get("reminders", {}), inc.get("reminders", {}))

    for key in ["goals", "skills", "stages", "milestones", "tasks", "insights"]:
        merged[key] = dict(base.get(key, {}))
        merged[key].update(inc.get(key, {}))

    merged["created_at"] = base.get("created_at") or inc.get("created_at")
    merged["updated_at"] = datetime.now(timezone.utc).isoformat()
    return _normalize_user_payload(merged, force_updated=False)


def _merge_reminders(base: Dict[str, Any], incoming: Any) -> Dict[str, Any]:
    normalized = default_reminders()

    if isinstance(base, dict):
        for key, value in base.items():
            if key in normalized and isinstance(value, dict):
                normalized[key].update(value)
            else:
                normalized[key] = value

    if isinstance(incoming, dict):
        for key, value in incoming.items():
            if key in normalized and isinstance(value, dict):
                normalized[key].update(value)
            else:
                normalized[key] = value

    return normalized


def _normalize_db_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    data = dict(payload)
    data.setdefault("version", 1)
    data.setdefault("created_at", datetime.now(timezone.utc).isoformat())
    data.setdefault("updated_at", datetime.now(timezone.utc).isoformat())

    chats = data.get("chats")
    if not isinstance(chats, dict):
        chats = {}

    normalized_chats: Dict[str, Any] = {}
    for chat_id, chat in chats.items():
        if not isinstance(chat, dict):
            normalized_chats[str(chat_id)] = {"users": {}}
            continue
        users = chat.get("users")
        if not isinstance(users, dict):
            users = {}
        normalized_users = {
            str(user_id): _normalize_user_payload(user_payload)
            for user_id, user_payload in users.items()
            if isinstance(user_payload, dict)
        }
        normalized_chats[str(chat_id)] = {"users": normalized_users}

    data["chats"] = normalized_chats
    return data


def _migrate_scopes_payload(user: Dict[str, Any]) -> None:
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

    user.pop("scopes", None)


def _merge_db_payload(base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    base_db = _normalize_db_payload(base)
    incoming_db = _normalize_db_payload(incoming)

    for chat_id, chat in incoming_db.get("chats", {}).items():
        dest_chat = base_db.setdefault("chats", {}).setdefault(chat_id, {"users": {}})
        dest_users = dest_chat.setdefault("users", {})
        users = chat.get("users", {}) if isinstance(chat, dict) else {}
        for user_id, user_payload in users.items():
            if user_id in dest_users:
                dest_users[user_id] = _merge_user_payload(dest_users[user_id], user_payload)
            else:
                dest_users[user_id] = _normalize_user_payload(user_payload)

    base_db["updated_at"] = datetime.now(timezone.utc).isoformat()
    return base_db


def _parse_name_desc(raw: str) -> Tuple[str, str]:
    if "|" in raw:
        name, desc = raw.split("|", 1)
        return name.strip(), desc.strip()
    return raw.strip(), ""


def _parse_goal_with_milestones(raw: str) -> Tuple[str, str, List[Dict[str, Any]]]:
    parts = [part.strip() for part in raw.split("|") if part.strip()]
    if not parts:
        return "", "", []

    name = parts[0]
    description = parts[1] if len(parts) > 1 else ""
    milestone_raw = " | ".join(parts[2:]) if len(parts) > 2 else ""
    milestones = _parse_milestones_list(milestone_raw) if milestone_raw else []
    return name, description, milestones


def _parse_milestones_list(raw: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for entry in [part.strip() for part in raw.split(",") if part.strip()]:
        name, percent = _extract_percent(entry)
        if not name:
            continue
        payload: Dict[str, Any] = {"name": name}
        if percent is not None:
            payload["target_percent"] = percent
            payload["description"] = f"Target: {percent}%"
        items.append(payload)
    return items


def _extract_percent(text: str) -> Tuple[str, int | None]:
    match = re.search(r"(\d{1,3})\s*%", text)
    percent = None
    if match:
        try:
            percent = int(match.group(1))
        except ValueError:
            percent = None
        text = re.sub(r"\s*\d{1,3}\s*%", " ", text, count=1)
    cleaned = " ".join(text.split())
    if percent is not None:
        percent = max(0, min(100, percent))
    return cleaned, percent


def _parse_name_kv(raw: str) -> Tuple[str, Dict[str, str]]:
    parts = [part.strip() for part in raw.split("|") if part.strip()]
    name = parts[0] if parts else ""
    kv: Dict[str, str] = {}
    for part in parts[1:]:
        if "=" not in part:
            continue
        key, _, value = part.partition("=")
        kv[key.strip().lower()] = value.strip()
    return name, kv


def _parse_int(raw: str | None, default: int = 1) -> int:
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _is_time_str(raw: str) -> bool:
    if ":" not in raw:
        return False
    parts = raw.split(":", 1)
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return False
    return 0 <= hour <= 23 and 0 <= minute <= 59


def _format_items(title: str, items: Dict[str, Any], include_status: bool = False) -> str:
    visible = [
        (item_id, payload)
        for item_id, payload in items.items()
        if not _is_hidden(payload)
    ]
    if not visible:
        return f"{title}: (empty)"

    lines = [f"{title} ({len(visible)}):"]
    for item_id, payload in visible:
        if not isinstance(payload, dict):
            lines.append(f"- {item_id}")
            continue
        name = payload.get("name", "")
        status = payload.get("status") if include_status else None
        suffix = f" ({status})" if status else ""
        lines.append(f"- {item_id}: {name}{suffix}")
    return "\n".join(lines)


def _normalize_text(value: str) -> str:
    return " ".join(str(value).strip().lower().split())


def _resolve_goal_id(user: Dict[str, Any], raw: str) -> Tuple[str | None, str | None]:
    goals = user.get("goals", {})
    if raw in goals:
        return raw, None
    matches = [
        goal_id
        for goal_id, goal in goals.items()
        if _normalize_text(goal.get("name", "")) == _normalize_text(raw)
    ]
    if len(matches) == 1:
        return matches[0], None
    if not matches:
        return None, "Goal not found."
    return None, "Multiple goals match that name. Use the goal id."


def _resolve_milestone_id(user: Dict[str, Any], raw: str) -> Tuple[str | None, str | None]:
    milestones = user.get("milestones", {})
    if raw in milestones:
        return raw, None
    matches = [
        milestone_id
        for milestone_id, milestone in milestones.items()
        if _normalize_text(milestone.get("name", "")) == _normalize_text(raw)
    ]
    if len(matches) == 1:
        return matches[0], None
    if not matches:
        return None, "Milestone not found."
    return None, "Multiple milestones match that name. Use the milestone id."


def _ensure_milestone_container(
    user: Dict[str, Any],
    goal_id: str,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    skills = user.setdefault("skills", {})
    stages = user.setdefault("stages", {})
    goals = user.setdefault("goals", {})
    goal = goals.get(goal_id)

    skill = None
    for payload in skills.values():
        if payload.get("goal_id") == goal_id and _normalize_text(payload.get("name", "")) == "milestones":
            skill = payload
            break

    if not skill:
        skill = new_skill(goal_id, "Milestones", "Auto-created for goal milestones")
        skills[skill["id"]] = skill
        if isinstance(goal, dict):
            goal.setdefault("skill_ids", []).append(skill["id"])

    stage = None
    for payload in stages.values():
        if payload.get("skill_id") == skill["id"] and _normalize_text(payload.get("name", "")) == "milestones":
            stage = payload
            break

    if not stage:
        stage = new_stage(skill["id"], "Milestones", "Auto-created for goal milestones")
        stages[stage["id"]] = stage
        skill.setdefault("stage_ids", []).append(stage["id"])

    return skill, stage


def _format_milestones_with_progress(user: Dict[str, Any]) -> str:
    milestones = user.get("milestones", {})
    visible = {
        milestone_id: payload
        for milestone_id, payload in milestones.items()
        if not _is_hidden(payload)
    }
    if not visible:
        return "Milestones: (empty)"

    lines = [f"Milestones ({len(visible)}):"]
    for milestone_id, payload in visible.items():
        if not isinstance(payload, dict):
            lines.append(f"- {milestone_id}")
            continue
        name = payload.get("name", "")
        percent = progress_for_milestone(user, milestone_id)
        lines.append(f"- {milestone_id}: {name} ({int(round(percent))}%)")
    return "\n".join(lines)


def _format_tasks_for_milestone(user: Dict[str, Any], milestone_id: str) -> str:
    milestones = user.get("milestones", {})
    milestone = milestones.get(milestone_id)
    if not isinstance(milestone, dict):
        return "Milestone not found."

    task_ids = milestone.get("task_ids", [])
    if not task_ids:
        return f"Tasks for {milestone_id}: (empty)"

    milestone_name = milestone.get("name", "")
    lines = [f"Tasks for {milestone_id}: {milestone_name}"]
    lines.extend(_format_task_lines(user, task_ids))
    return "\n".join(lines)


def _format_tasks_grouped(user: Dict[str, Any]) -> str:
    tasks = user.get("tasks", {})
    visible_tasks = {task_id: payload for task_id, payload in tasks.items() if not _is_hidden(payload)}
    if not visible_tasks:
        return "Tasks: (empty)"

    milestones = user.get("milestones", {})
    lines = ["Tasks:"]
    has_any = False
    for milestone_id, milestone in milestones.items():
        if not isinstance(milestone, dict):
            continue
        task_ids = milestone.get("task_ids", [])
        task_ids = [task_id for task_id in task_ids if task_id in visible_tasks]
        if not task_ids:
            continue
        has_any = True
        milestone_name = milestone.get("name", "")
        lines.append(f"{milestone_id}: {milestone_name}")
        lines.extend(_format_task_lines(user, task_ids))

    if not has_any:
        return "Tasks: (empty)"
    return "\n".join(lines)


def _format_task_lines(user: Dict[str, Any], task_ids: List[str]) -> List[str]:
    tasks = user.get("tasks", {})
    lines: List[str] = []
    for idx, task_id in enumerate(task_ids, start=1):
        task = tasks.get(task_id)
        if not isinstance(task, dict) or _is_hidden(task):
            continue
        name = task.get("name", "")
        status = task.get("status", "")
        lines.append(f"  #{idx} {name} ({status})")
    return lines


def _resolve_task_selector(
    user: Dict[str, Any],
    raw: str,
) -> Tuple[str | None, str | None]:
    raw = raw.strip()
    if not raw:
        return None, "Please provide a task id, or milestone + task selector."

    if "|" in raw:
        scope_ref, selector = raw.split("|", 1)
        scope_id, error = _resolve_milestone_id(user, scope_ref.strip())
        if error:
            return None, error
        return _resolve_task_in_milestone(user, scope_id, selector.strip())

    tasks = user.get("tasks", {})
    if raw in tasks:
        return raw, None

    task_ids = list(tasks.keys())
    task_id, error = _resolve_task_by_name(tasks, task_ids, raw, "all tasks")
    if error:
        if error.startswith("Multiple tasks"):
            return None, "Multiple tasks match. Use /complete_task <milestone_id> | <name or #index>."
        return None, error
    return task_id, None


def _resolve_task_in_milestone(
    user: Dict[str, Any],
    milestone_id: str,
    selector: str,
) -> Tuple[str | None, str | None]:
    milestones = user.get("milestones", {})
    milestone = milestones.get(milestone_id)
    if not isinstance(milestone, dict):
        return None, "Milestone not found."

    task_ids = milestone.get("task_ids", [])
    if not task_ids:
        return None, "Milestone has no tasks."

    normalized = selector.lstrip("#").strip()
    if normalized.isdigit():
        index = int(normalized)
        if index < 1 or index > len(task_ids):
            return None, f"Task index out of range. Use 1-{len(task_ids)}."
        return task_ids[index - 1], None

    tasks = user.get("tasks", {})
    return _resolve_task_by_name(tasks, task_ids, selector, f"milestone {milestone_id}")


def _resolve_task_by_name(
    tasks: Dict[str, Any],
    task_ids: List[str],
    selector: str,
    label: str,
) -> Tuple[str | None, str | None]:
    selector_norm = _normalize_text(selector)
    exact_matches = [
        task_id
        for task_id in task_ids
        if _normalize_text(tasks.get(task_id, {}).get("name", "")) == selector_norm
    ]
    if len(exact_matches) == 1:
        return exact_matches[0], None
    if len(exact_matches) > 1:
        return None, f"Multiple tasks named '{selector}' in {label}. Use #index."

    partial_matches = [
        task_id
        for task_id in task_ids
        if selector_norm in _normalize_text(tasks.get(task_id, {}).get("name", ""))
    ]
    if len(partial_matches) == 1:
        return partial_matches[0], None
    if len(partial_matches) > 1:
        return None, f"Multiple tasks match '{selector}' in {label}. Use #index."

    return None, f"Task '{selector}' not found in {label}."


def _milestone_progress_for_task(user: Dict[str, Any], task: Dict[str, Any]) -> float | None:
    milestone_id = task.get("milestone_id")
    if not milestone_id:
        return None
    return progress_for_milestone(user, milestone_id)


def _parse_tags(raw: str | None) -> List[str]:
    if not raw:
        return []
    lowered = _normalize_text(raw)
    if lowered in RESET_WORDS or lowered in REMOVE_WORDS:
        return []
    tags: List[str] = []
    seen = set()
    for part in raw.split(","):
        tag = part.strip()
        if not tag:
            continue
        key = _normalize_text(tag)
        if key in seen:
            continue
        seen.add(key)
        tags.append(tag)
    return tags


def _filter_insights(insights: Dict[str, Any], mode: str) -> List[Tuple[str, Dict[str, Any]]]:
    items: List[Tuple[str, Dict[str, Any]]] = []
    for insight_id, insight in insights.items():
        if not isinstance(insight, dict):
            continue
        has_tags = bool(insight.get("tags"))
        has_summary = bool(insight.get("summary"))

        if mode == "all":
            items.append((insight_id, insight))
        elif mode == "untagged" and not has_tags:
            items.append((insight_id, insight))
        elif mode == "unsummarized" and not has_summary:
            items.append((insight_id, insight))
        elif mode == "pending" and (not has_tags or not has_summary):
            items.append((insight_id, insight))

    return sorted(items, key=lambda item: item[1].get("created_at", ""))


def _format_insight_items(items: List[Tuple[str, Dict[str, Any]]], mode: str) -> str:
    lines = [f"Insights ({mode}, {len(items)}):"]
    for insight_id, insight in items:
        lines.append(_format_insight_line(insight_id, insight))
    return "\n".join(lines)


def _format_insight_line(insight_id: str, insight: Dict[str, Any]) -> str:
    text = _truncate_text(insight.get("text", ""), 80)
    group = insight.get("group", "")
    tags = insight.get("tags", [])
    summary = insight.get("summary", "")

    meta_parts = []
    if group:
        meta_parts.append(f"group={group}")
    meta_parts.append("tags=" + (",".join(tags) if tags else "none"))
    meta_parts.append("summary=" + ("ok" if summary else "missing"))

    return f"- {insight_id}: {text} ({', '.join(meta_parts)})"


def _truncate_text(text: str, limit: int) -> str:
    cleaned = " ".join(str(text).split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: max(0, limit - 3)] + "..."


def _progress_for_entity(user: Dict[str, Any], entity_type: str, entity_id: str) -> float | None:
    if entity_type == "goal":
        return progress_for_goal(user, entity_id)
    if entity_type == "skill":
        return progress_for_skill(user, entity_id)
    if entity_type == "stage":
        return progress_for_stage(user, entity_id)
    if entity_type == "milestone":
        return progress_for_milestone(user, entity_id)
    if entity_type == "task":
        tasks = user.get("tasks", {})
        if entity_id in tasks:
            return progress_for_task_ids(user, [entity_id])
    return None


def _load_user(update: Update) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    db = load_db()
    user = get_user_data(db, update.effective_chat.id, update.effective_user.id)
    return db, user


def _parse_weekday(value: str) -> int | None:
    value = value.strip().lower()
    mapping = {
        "mon": 0,
        "monday": 0,
        "tue": 1,
        "tues": 1,
        "tuesday": 1,
        "wed": 2,
        "wednesday": 2,
        "thu": 3,
        "thur": 3,
        "thurs": 3,
        "thursday": 3,
        "fri": 4,
        "friday": 4,
        "sat": 5,
        "saturday": 5,
        "sun": 6,
        "sunday": 6,
    }
    if value in mapping:
        return mapping[value]
    try:
        num = int(value)
        return num if 0 <= num <= 6 else None
    except ValueError:
        return None


def _format_reminder_status(reminders: Dict[str, Any]) -> str:
    daily = reminders.get("daily", {}) if isinstance(reminders, dict) else {}
    weekly = reminders.get("weekly", {}) if isinstance(reminders, dict) else {}

    daily_enabled = bool(daily.get("enabled"))
    weekly_enabled = bool(weekly.get("enabled"))

    daily_time = daily.get("time", "--:--")
    weekly_time = weekly.get("time", "--:--")
    weekly_day = weekly.get("weekday", "-")

    return (
        "Reminder status:\n"
        f"daily: {'on' if daily_enabled else 'off'} at {daily_time}\n"
        f"weekly: {'on' if weekly_enabled else 'off'} on {weekly_day} at {weekly_time}\n"
        "\nUsage:\n"
        "/remind daily HH:MM\n"
        "/remind weekly <mon|tue|...|0-6> HH:MM\n"
        "/remind off\n"
        "/remind status"
    )
