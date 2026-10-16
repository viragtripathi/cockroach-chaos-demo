"""Recent happenings for the stage ticker and the presenter log."""
from __future__ import annotations

import itertools
import threading
import time
from collections import deque


class Events:
    def __init__(self, limit: int = 50, clock=time.time):
        self._items: deque[dict] = deque(maxlen=limit)
        self._lock = threading.Lock()
        self._ids = itertools.count(1)
        self._clock = clock

    def publish(self, text: str, level: str = "info") -> None:
        """level: info | warn | bad | good"""
        with self._lock:
            self._items.append({"id": next(self._ids), "ts": self._clock(), "text": text, "level": level})

    def recent(self, n: int | None = None) -> list[dict]:
        with self._lock:
            items = list(self._items)
        return items[-n:] if n else items
