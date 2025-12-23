from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict

import orjson

from ..config import PROJECT_ROOT


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as a JSON line suitable for structured ingestion."""
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, self.datefmt),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        try:
            return orjson.dumps(payload).decode()
        except Exception:  # noqa: BLE001
            return json.dumps(payload)


def setup_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_json = os.environ.get("LOG_JSON", "1") != "0"
    handler = logging.StreamHandler(sys.stdout)
    formatter: logging.Formatter
    if log_json:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
    log_path = Path(os.environ.get("LOG_FILE") or str(PROJECT_ROOT / "polycopy.log"))
    try:
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    except OSError:
        pass
