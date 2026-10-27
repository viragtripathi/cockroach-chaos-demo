"""Runtime preferences that survive restarts: latency mode, booth settings, trigger map and counters."""
from __future__ import annotations

import copy
import json
import os
import tempfile
import threading
from pathlib import Path

DEFAULTS = {
    "latency": None,  # None -> use the preset's latency setting
    "booth": {"enabled": False, "auto_heal_s": 45},
    "triggers": {
        "roach-1": {"action": "crash", "target": "random-safe"},
        "roach-2": {"action": "crash", "target": "random-region"},
        "big-red": {"action": "recover", "target": "all"},
    },
    "keys": {"1": "roach-1", "2": "roach-2", "9": "big-red"},
    "counters": {"nodes_crashed": 0, "regions_down": 0},
}


class Store:
    def __init__(self, data_dir: Path):
        self._path = data_dir / "settings.json"
        self._lock = threading.Lock()
        self._data = copy.deepcopy(DEFAULTS)
        try:
            self._data.update(json.loads(self._path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            pass

    def get(self, key: str):
        with self._lock:
            return copy.deepcopy(self._data.get(key))

    def all(self) -> dict:
        with self._lock:
            return copy.deepcopy(self._data)

    def set(self, key: str, value) -> None:
        with self._lock:
            self._data[key] = copy.deepcopy(value)
            self._path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=self._path.parent, prefix=".settings-", suffix=".json")
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2)
            os.replace(tmp, self._path)
