from __future__ import annotations

from progress_tracker.config import load_env
from progress_tracker.cli import run


if __name__ == "__main__":
    load_env()
    raise SystemExit(run())
