from __future__ import annotations

import argparse
from typing import Any, Dict, Iterable, List

from .models import (
    STATUS_DONE,
    TASK_KIND_EXPERIMENT,
    TASK_KIND_TASK,
    ensure_user,
    new_goal,
    new_milestone,
    new_skill,
    new_stage,
    new_task,
    touch,
)
from .progress import (
    progress_for_goal,
    progress_for_milestone,
    progress_for_skill,
    progress_for_stage,
    render_progress_block,
    resolve_render_settings,
)
from .storage import load_db, save_db

DEFAULT_CHAT_ID = "local"
DEFAULT_USER_ID = "local"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="MGT Progress Tracker CLI")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("list-goals")
    add_goal = subparsers.add_parser("add-goal")
    add_goal.add_argument("name")
    add_goal.add_argument("--description", default="")

    subparsers.add_parser("list-skills")
    add_skill = subparsers.add_parser("add-skill")
    add_skill.add_argument("goal_id")
    add_skill.add_argument("name")
    add_skill.add_argument("--description", default="")

    subparsers.add_parser("list-stages")
    add_stage = subparsers.add_parser("add-stage")
    add_stage.add_argument("skill_id")
    add_stage.add_argument("name")
    add_stage.add_argument("--description", default="")

    subparsers.add_parser("list-milestones")
    add_milestone = subparsers.add_parser("add-milestone")
    add_milestone.add_argument("stage_id")
    add_milestone.add_argument("name")
    add_milestone.add_argument("--description", default="")

    subparsers.add_parser("list-tasks")
    add_task = subparsers.add_parser("add-task")
    add_task.add_argument("milestone_id")
    add_task.add_argument("name")
    add_task.add_argument("--kind", choices=["task", "experiment"], default="task")
    add_task.add_argument("--weight", type=int, default=1)

    complete_task = subparsers.add_parser("complete-task")
    complete_task.add_argument("task_id")

    progress = subparsers.add_parser("progress")
    progress.add_argument("entity_type", choices=["goal", "skill", "stage", "milestone"])
    progress.add_argument("entity_id")

    return parser


def run(argv: List[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    db = load_db()
    user = ensure_user(db, DEFAULT_CHAT_ID, DEFAULT_USER_ID)

    if args.command == "list-goals":
        _print_items("Goals", user.get("goals", {}))
    elif args.command == "add-goal":
        goal = new_goal(args.name, args.description)
        user["goals"][goal["id"]] = goal
        touch(user)
        save_db(db)
        _print_items("Goals", user.get("goals", {}))
    elif args.command == "list-skills":
        _print_items("Skills", user.get("skills", {}))
    elif args.command == "add-skill":
        goal = user.get("goals", {}).get(args.goal_id)
        if not goal:
            print("Goal not found")
            return 1
        skill = new_skill(args.goal_id, args.name, args.description)
        user["skills"][skill["id"]] = skill
        goal.setdefault("skill_ids", []).append(skill["id"])
        touch(goal)
        touch(user)
        save_db(db)
        _print_items("Skills", user.get("skills", {}))
    elif args.command == "list-stages":
        _print_items("Stages", user.get("stages", {}))
    elif args.command == "add-stage":
        skill = user.get("skills", {}).get(args.skill_id)
        if not skill:
            print("Skill not found")
            return 1
        stage = new_stage(args.skill_id, args.name, args.description)
        user["stages"][stage["id"]] = stage
        skill.setdefault("stage_ids", []).append(stage["id"])
        touch(skill)
        touch(user)
        save_db(db)
        _print_items("Stages", user.get("stages", {}))
    elif args.command == "list-milestones":
        _print_items("Milestones", user.get("milestones", {}))
    elif args.command == "add-milestone":
        stage = user.get("stages", {}).get(args.stage_id)
        if not stage:
            print("Stage not found")
            return 1
        milestone = new_milestone(args.stage_id, args.name, args.description)
        user["milestones"][milestone["id"]] = milestone
        stage.setdefault("milestone_ids", []).append(milestone["id"])
        touch(stage)
        touch(user)
        save_db(db)
        _print_items("Milestones", user.get("milestones", {}))
    elif args.command == "list-tasks":
        _print_items("Tasks", user.get("tasks", {}))
    elif args.command == "add-task":
        milestone = user.get("milestones", {}).get(args.milestone_id)
        if not milestone:
            print("Milestone not found")
            return 1
        kind = TASK_KIND_TASK if args.kind == "task" else TASK_KIND_EXPERIMENT
        task = new_task(args.milestone_id, args.name, kind, args.weight)
        user["tasks"][task["id"]] = task
        milestone.setdefault("task_ids", []).append(task["id"])
        touch(milestone)
        touch(user)
        save_db(db)
        _print_items("Tasks", user.get("tasks", {}))
    elif args.command == "complete-task":
        task = user.get("tasks", {}).get(args.task_id)
        if not task:
            print("Task not found")
            return 1
        task["status"] = STATUS_DONE
        touch(task)
        touch(user)
        save_db(db)
        print(f"Completed {args.task_id}")
    elif args.command == "progress":
        settings = resolve_render_settings(user.get("settings", {}))
        percent = _progress_for_entity(user, args.entity_type, args.entity_id)
        label = f"{args.entity_type}:{args.entity_id}"
        print(render_progress_block(label, percent, settings))
    else:
        parser.print_help()

    return 0


def _progress_for_entity(user: Dict[str, Any], entity_type: str, entity_id: str) -> float:
    if entity_type == "goal":
        return progress_for_goal(user, entity_id)
    if entity_type == "skill":
        return progress_for_skill(user, entity_id)
    if entity_type == "stage":
        return progress_for_stage(user, entity_id)
    if entity_type == "milestone":
        return progress_for_milestone(user, entity_id)
    return 0.0


def _print_items(title: str, items: Dict[str, Any]) -> None:
    print(f"{title} ({len(items)}):")
    for item_id, payload in items.items():
        name = payload.get("name", "") if isinstance(payload, dict) else ""
        print(f"- {item_id}: {name}")
