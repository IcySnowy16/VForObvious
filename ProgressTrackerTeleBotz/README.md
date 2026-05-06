# MGT Progress Tracker (Telegram + CLI)

A simple progress tracker to learn Telegram bot development while tracking goals, skills, stages, milestones, and daily tasks.

## What this MVP includes
- Telegram bot commands to add goals, skills, tasks, and mark completion
- Text-based progress visualization (ASCII bars)
- Local JSON storage for easy manual edits
- Optional daily/weekly reminders (Asia/Singapore)
- A CLI for fast offline edits

## Project layout
- progress_tracker/config.py - env + defaults
- progress_tracker/models.py - data schema and helpers
- progress_tracker/storage.py - load/save JSON
- progress_tracker/progress.py - aggregation + rendering
- progress_tracker/cli.py - CLI commands
- progress_tracker/bot.py - Telegram bot commands
- progress_tracker/reminders.py - reminder scheduling
- run_cli.py - CLI entrypoint
- run_bot.py - bot entrypoint

## Quick start
1) Create and activate a virtual environment, then install deps:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2) Create a .env file:

```
TELEGRAM_TOKEN=your_token_here
TIMEZONE=Asia/Singapore
DATA_FILE=data/progress_data.json
```

3) Run the CLI or the bot:

```powershell
python run_cli.py --help
python run_bot.py
```

## Progress visualization format
The default renderer uses ASCII symbols so it works everywhere:

```
[x]--[x]--[>]--[ ]--[ ]
10%  Done Skill A
30%  In-Progress Skill B
100% Done Skill C
```

You can customize the symbols later in progress_tracker/config.py.

## Extend it later
- Add new fields to the data schema in progress_tracker/models.py
- Add new commands in progress_tracker/cli.py and progress_tracker/bot.py
- Swap JSON storage for SQLite in progress_tracker/storage.py

## Notes
- This repo already contains Telegram bot examples under Telegram/ and Old For Experiment/.
- The data file is stored in data/progress_data.json; do not commit it.
