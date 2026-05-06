from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any, Dict, List, Tuple

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from .config import get_token, load_env
from .models import (
    STATUS_DONE,
    TASK_KIND_EXPERIMENT,
    TASK_KIND_TASK,
    new_goal,
    new_insight,
    new_milestone,
    new_scope,
    new_skill,
    new_stage,
    new_task,
    touch,
)
from .progress import (
    progress_for_goal,
    progress_for_milestone,
    progress_for_scope,
    progress_for_skill,
    progress_for_stage,
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

STATE_MILESTONE_POSITION = 1
STATE_MILESTONE_EMOJI = 2
STATE_MILESTONE_SYMBOLS = 3

HELP_TEXT = (
    "Commands:\n"
    "/start - Show help\n"
    "/ping - Health check\n"
    "/add_updates <text> - Log update requests\n"
    "/add_goal <name> [| desc] [| milestone1 50%, milestone2 80%]\n"
    "/goal_to_milestones <goal_id|goal_name> | milestone1 50%, milestone2 80%\n"
    "/list_goals\n"
    "/add_skill <goal_id> <name> [| desc]\n"
    "/list_skills\n"
    "/add_stage <skill_id> <name> [| desc]\n"
    "/list_stages\n"
    "/add_milestone <stage_id> <name> [| desc]\n"
    "/list_milestones\n"
    "/add_scope <milestone_id> <name> [| start=YYYY-MM-DD | end=YYYY-MM-DD]\n"
    "/list_scopes\n"
    "/add_task <scope_id> <name> [| kind=task|experiment | weight=2]\n"
    "/list_tasks [scope_id]\n"
    "/complete_task <task_id>\n"
    "/complete_task <scope_id> | <task name or #index>\n"
    "/progress <goal|skill|stage|milestone|scope> <id>\n"
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
    "/view_settings - Show current render settings\n\n"
    "Examples:\n"
    "/add_goal Learn Japanese | Long term goal\n"
    "/add_goal Stress Management | Long term goal | Failure Mgmt 50%, Task Manager 80%\n"
    "/goal_to_milestones Stress Management | Failure Mgmt 50%, Task Manager 80%\n"
    "/add_skill goal_123 Kana Recognition\n"
    "/add_task scope_123 Drill 10 mins | kind=task | weight=1\n"
    "/list_tasks scope_123\n"
    "/complete_task scope_123 | #2\n"
    "/add_insight Daily review felt rushed | tags=process,energy | group=weekly\n"
    "/progress skill skill_123\n"
    "/remind daily 20:00\n"
    "/remind weekly mon 09:00\n"
    "/set_milestone\n"
    "/set_milestones 25,50,75\n"
    "/set_symbols done=[x] doing=[>] milestone=[*]\n"
    "/set_emoji done=OK doing=WORK milestone=STAR\n"
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


async def cmd_add_scope(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /add_scope <milestone_id> <name> [| start=YYYY-MM-DD | end=YYYY-MM-DD]"
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

    scope = new_scope(milestone_id, name, kv.get("start", ""), kv.get("end", ""))
    user["scopes"][scope["id"]] = scope
    milestone.setdefault("scope_ids", []).append(scope["id"])
    touch(milestone)
    touch(user)
    save_db(db)
    await update.message.reply_text(f"Scope added: {scope['id']} - {scope['name']}")


async def cmd_list_scopes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _, user = _load_user(update)
    msg = _format_items("Scopes", user.get("scopes", {}))
    await update.message.reply_text(msg)


async def cmd_add_task(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) < 2:
        await update.message.reply_text(
            "Usage: /add_task <scope_id> <name> [| kind=task|experiment | weight=2]"
        )
        return

    scope_id = context.args[0]
    name, kv = _parse_name_kv(" ".join(context.args[1:]))
    if not name:
        await update.message.reply_text("Please provide a name.")
        return

    db, user = _load_user(update)
    scope = user.get("scopes", {}).get(scope_id)
    if not scope:
        await update.message.reply_text("Scope not found.")
        return

    kind_raw = (kv.get("kind") or "task").lower()
    kind = TASK_KIND_TASK if kind_raw != "experiment" else TASK_KIND_EXPERIMENT
    weight = _parse_int(kv.get("weight"), default=1)

    task = new_task(scope_id, name, kind, weight)
    user["tasks"][task["id"]] = task
    scope.setdefault("task_ids", []).append(task["id"])
    touch(scope)
    touch(user)
    save_db(db)
    await update.message.reply_text(f"Task added: {task['id']} - {task['name']}")


async def cmd_list_tasks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    _, user = _load_user(update)
    if context.args:
        scope_ref = " ".join(context.args).strip()
        scope_id, error = _resolve_scope_id(user, scope_ref)
        if error:
            await update.message.reply_text(error)
            return
        msg = _format_tasks_for_scope(user, scope_id)
    else:
        msg = _format_tasks_grouped(user)
    await update.message.reply_text(msg)


async def cmd_complete_task(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: /complete_task <task_id> OR /complete_task <scope_id> | <task name or #index>"
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


async def cmd_progress(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if len(context.args) != 2:
        await update.message.reply_text(
            "Usage: /progress <goal|skill|stage|milestone|scope> <id>"
        )
        return

    entity_type = context.args[0].lower()
    entity_id = context.args[1]
    db, user = _load_user(update)

    percent = _progress_for_entity(user, entity_type, entity_id)
    if percent is None:
        await update.message.reply_text("Unknown entity type.")
        return

    settings = resolve_render_settings(user.get("settings", {}))
    label = f"{entity_type}:{entity_id}"
    block = render_progress_block(label, percent, settings)
    await update.message.reply_text(block)


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
    await update.message.reply_text("Setup cancelled.")
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

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("ping", cmd_ping))
    app.add_handler(CommandHandler("add_updates", cmd_add_updates))
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
    app.add_handler(CommandHandler("remind", cmd_remind))
    app.add_handler(milestone_flow)
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
    if not items:
        return f"{title}: (empty)"

    lines = [f"{title} ({len(items)}):"]
    for item_id, payload in items.items():
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


def _resolve_scope_id(user: Dict[str, Any], raw: str) -> Tuple[str | None, str | None]:
    scopes = user.get("scopes", {})
    if raw in scopes:
        return raw, None
    matches = [
        scope_id
        for scope_id, scope in scopes.items()
        if _normalize_text(scope.get("name", "")) == _normalize_text(raw)
    ]
    if len(matches) == 1:
        return matches[0], None
    if not matches:
        return None, "Scope not found."
    return None, "Multiple scopes match that name. Use the scope id."


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
    if not milestones:
        return "Milestones: (empty)"

    lines = [f"Milestones ({len(milestones)}):"]
    for milestone_id, payload in milestones.items():
        if not isinstance(payload, dict):
            lines.append(f"- {milestone_id}")
            continue
        name = payload.get("name", "")
        percent = progress_for_milestone(user, milestone_id)
        lines.append(f"- {milestone_id}: {name} ({int(round(percent))}%)")
    return "\n".join(lines)


def _format_tasks_for_scope(user: Dict[str, Any], scope_id: str) -> str:
    scopes = user.get("scopes", {})
    scope = scopes.get(scope_id)
    if not isinstance(scope, dict):
        return "Scope not found."

    task_ids = scope.get("task_ids", [])
    if not task_ids:
        return f"Tasks for {scope_id}: (empty)"

    scope_name = scope.get("name", "")
    lines = [f"Tasks for {scope_id}: {scope_name}"]
    lines.extend(_format_task_lines(user, task_ids))
    return "\n".join(lines)


def _format_tasks_grouped(user: Dict[str, Any]) -> str:
    tasks = user.get("tasks", {})
    if not tasks:
        return "Tasks: (empty)"

    scopes = user.get("scopes", {})
    lines = ["Tasks:"]
    has_any = False
    for scope_id, scope in scopes.items():
        if not isinstance(scope, dict):
            continue
        task_ids = scope.get("task_ids", [])
        if not task_ids:
            continue
        has_any = True
        scope_name = scope.get("name", "")
        lines.append(f"{scope_id}: {scope_name}")
        lines.extend(_format_task_lines(user, task_ids))

    if not has_any:
        return "Tasks: (empty)"
    return "\n".join(lines)


def _format_task_lines(user: Dict[str, Any], task_ids: List[str]) -> List[str]:
    tasks = user.get("tasks", {})
    lines: List[str] = []
    for idx, task_id in enumerate(task_ids, start=1):
        task = tasks.get(task_id)
        if not isinstance(task, dict):
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
        return None, "Please provide a task id, or scope + task selector."

    if "|" in raw:
        scope_ref, selector = raw.split("|", 1)
        scope_id, error = _resolve_scope_id(user, scope_ref.strip())
        if error:
            return None, error
        return _resolve_task_in_scope(user, scope_id, selector.strip())

    tasks = user.get("tasks", {})
    if raw in tasks:
        return raw, None

    task_ids = list(tasks.keys())
    task_id, error = _resolve_task_by_name(tasks, task_ids, raw, "all tasks")
    if error:
        if error.startswith("Multiple tasks"):
            return None, "Multiple tasks match. Use /complete_task <scope_id> | <name or #index>."
        return None, error
    return task_id, None


def _resolve_task_in_scope(
    user: Dict[str, Any],
    scope_id: str,
    selector: str,
) -> Tuple[str | None, str | None]:
    scopes = user.get("scopes", {})
    scope = scopes.get(scope_id)
    if not isinstance(scope, dict):
        return None, "Scope not found."

    task_ids = scope.get("task_ids", [])
    if not task_ids:
        return None, "Scope has no tasks."

    normalized = selector.lstrip("#").strip()
    if normalized.isdigit():
        index = int(normalized)
        if index < 1 or index > len(task_ids):
            return None, f"Task index out of range. Use 1-{len(task_ids)}."
        return task_ids[index - 1], None

    tasks = user.get("tasks", {})
    return _resolve_task_by_name(tasks, task_ids, selector, f"scope {scope_id}")


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
    scope_id = task.get("scope_id")
    if not scope_id:
        return None
    scope = user.get("scopes", {}).get(scope_id)
    if not isinstance(scope, dict):
        return None
    milestone_id = scope.get("milestone_id")
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
    if entity_type == "scope":
        return progress_for_scope(user, entity_id)
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
