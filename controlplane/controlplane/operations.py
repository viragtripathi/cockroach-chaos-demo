"""Tracks the one long-running cluster operation (provision, rebuild, adopt) for the stage and API."""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field


@dataclass
class Operation:
    id: str
    kind: str
    status: str = "running"  # running | done | failed
    steps: list[str] = field(default_factory=list)
    error: str | None = None
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    in_progress: bool = False  # the last step is a progress line that the next progress() replaces

    def to_dict(self) -> dict:
        return {"id": self.id, "kind": self.kind, "status": self.status, "steps": list(self.steps),
                "error": self.error, "started_at": self.started_at, "finished_at": self.finished_at}


class OperationBusy(RuntimeError):
    """Another operation is still running."""


class Operations:
    def __init__(self):
        self._lock = threading.Lock()
        self._current: Operation | None = None

    def start(self, kind: str) -> Operation:
        with self._lock:
            if self._current and self._current.status == "running":
                raise OperationBusy(f"{self._current.kind} already running")
            self._current = Operation(id=uuid.uuid4().hex[:8], kind=kind)
            return self._current

    def step(self, op: Operation, text: str) -> None:
        with self._lock:
            op.steps.append(text)
            op.in_progress = False

    def progress(self, op: Operation, text: str) -> None:
        with self._lock:
            if op.in_progress and op.steps:
                op.steps[-1] = text
            else:
                op.steps.append(text)
            op.in_progress = True

    def finish(self, op: Operation, error: str | None = None) -> None:
        with self._lock:
            op.status = "failed" if error else "done"
            op.error = error
            op.finished_at = time.time()

    def snapshot(self) -> dict | None:
        with self._lock:
            return self._current.to_dict() if self._current else None
