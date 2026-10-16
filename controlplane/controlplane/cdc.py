"""Webhook sink for the changefeed scenario: an HTTPS endpoint inside the cluster network that counts events."""
from __future__ import annotations

import json
import logging
import ssl
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

log = logging.getLogger(__name__)


def hlc_seconds(value: str) -> float | None:
    """'1760054400123456789.0000000001' -> wall-clock seconds."""
    try:
        return int(str(value).split(".", 1)[0]) / 1e9
    except (TypeError, ValueError):
        return None


class CdcSink:
    def __init__(self, clock=time.time):
        self._clock = clock
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self.events = 0
            self.resolved: float | None = None
            self.resolved_at: float | None = None
            self.max_gap = 0.0
            self.recent: deque = deque(maxlen=6)
            self._per_second: deque = deque(maxlen=12)

    def ingest(self, body: dict) -> None:
        now = self._clock()
        with self._lock:
            if "resolved" in body:
                ts = hlc_seconds(body["resolved"])
                if ts is not None and (self.resolved is None or ts > self.resolved):
                    if self.resolved_at is not None:
                        self.max_gap = max(self.max_gap, now - self.resolved_at)
                    self.resolved, self.resolved_at = ts, now
                return
            rows = body.get("payload") or []
            self.events += len(rows)
            sec = int(now)
            if self._per_second and self._per_second[-1][0] == sec:
                self._per_second[-1][1] += len(rows)
            else:
                self._per_second.append([sec, len(rows)])
            for row in rows[-3:]:
                after = row.get("after") or {}
                self.recent.append({"key": row.get("key"), "balance": after.get("balance")})

    def status(self) -> dict:
        now = self._clock()
        with self._lock:
            done = [n for s, n in self._per_second if s < int(now)][-5:]
            return {"events": self.events, "events_s": round(sum(done) / len(done)) if done else 0,
                    "resolved_lag_s": None if self.resolved is None else round(max(0.0, now - self.resolved), 1),
                    "max_gap_s": round(self.max_gap, 1), "recent": list(self.recent)}

    def serve(self, port: int, cert: Path, key: Path) -> None:
        sink = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                try:
                    sink.ingest(json.loads(self.rfile.read(length) or b"{}"))
                except json.JSONDecodeError:
                    pass
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(cert, key)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True, name="cdc-sink").start()
        log.info("changefeed sink listening on :%d", port)
