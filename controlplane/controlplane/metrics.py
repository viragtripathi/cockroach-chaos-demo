"""Cluster-side traffic from each node's /_status/vars: works for any client, including Banko."""
from __future__ import annotations

import logging
import threading
import time
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

log = logging.getLogger(__name__)

COUNTERS = {"sql_query_count": "stmt_s", "sql_txn_latency_count": "txn_s", "sql_txn_abort_count": "aborts_s",
            "sql_failure_count": "errors_s", "txn_restarts_sum": "retries_s"}
GAUGES = {"sql_conns": "conns"}
HISTORY_POINTS = 60  # 2 minutes at the 2 s poll interval


def parse_vars(text: str) -> tuple[dict[str, float], list[tuple[float, float]]]:
    scalars: dict[str, float] = {}
    buckets: list[tuple[float, float]] = []
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        try:
            head, value = line.rsplit(" ", 1)
            name = head.split("{", 1)[0]
            if name == "sql_service_latency_bucket":
                le = head.split('le="', 1)[1].split('"', 1)[0]
                buckets.append((float("inf") if le == "+Inf" else float(le), float(value)))
            elif name in COUNTERS or name in GAUGES:
                scalars[name] = float(value)
        except (ValueError, IndexError):
            continue
    buckets.sort()
    return scalars, buckets


def rate(prev: float, cur: float, dt: float) -> float:
    return 0.0 if cur < prev or dt <= 0 else (cur - prev) / dt


def histogram_quantile(q: float, prev: list[tuple[float, float]], cur: list[tuple[float, float]]) -> float | None:
    """Quantile in ms from the delta of two cumulative nanosecond histograms."""
    before = dict(prev)
    deltas = [(le, max(0.0, n - before.get(le, 0.0))) for le, n in cur]
    total = deltas[-1][1] if deltas else 0
    if total <= 0:
        return None
    rank, lower_le, lower_n = q * total, 0.0, 0.0
    for le, n in deltas:
        if n >= rank:
            if le == float("inf"):
                return lower_le / 1e6
            span = n - lower_n
            frac = (rank - lower_n) / span if span else 1.0
            return (lower_le + (le - lower_le) * frac) / 1e6
        lower_le, lower_n = le, n
    return lower_le / 1e6


def _fetch(ip: str) -> str | None:
    try:
        with urllib.request.urlopen(f"http://{ip}:8080/_status/vars", timeout=0.7) as resp:
            return resp.read().decode("utf-8", "replace")
    except Exception:
        return None


class Metrics:
    def __init__(self, *, fetch: Callable[[str], str | None] = _fetch, clock: Callable[[], float] = time.time,
                 interval: float = 2.0):
        self._fetch = fetch
        self._clock = clock
        self._interval = interval
        self._lock = threading.Lock()
        self._prev: dict[str, tuple[float, dict, list]] = {}
        self._status: dict = {"cluster": None, "regions": {}, "nodes": {}, "history": []}
        self._history: deque = deque(maxlen=HISTORY_POINTS)
        self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="metrics")
        self._stop = threading.Event()

    def poll(self, nodes: list[tuple[str, str, str]]) -> None:
        """nodes: (name, region, ip)."""
        now = self._clock()
        pages = list(self._pool.map(lambda n: self._fetch(n[2]), nodes))
        per_node: dict[str, dict | None] = {}
        prev_buckets, cur_buckets = [], []
        for (name, region, _), page in zip(nodes, pages):
            if page is None:
                per_node[name] = None
                self._prev.pop(name, None)
                continue
            scalars, buckets = parse_vars(page)
            last = self._prev.get(name)
            self._prev[name] = (now, scalars, buckets)
            if last is None:
                per_node[name] = None
                continue
            dt = now - last[0]
            row = {key: rate(last[1].get(m, 0.0), scalars.get(m, 0.0), dt) for m, key in COUNTERS.items()}
            row.update({key: scalars.get(m, 0.0) for m, key in GAUGES.items()})
            row["region"] = region
            per_node[name] = row
            prev_buckets.append(last[2])
            cur_buckets.append(buckets)

        def total(rows):
            keys = list(COUNTERS.values()) + list(GAUGES.values())
            return {k: round(sum(r[k] for r in rows), 1) for k in keys}

        rows = [r for r in per_node.values() if r]
        regions: dict[str, dict] = {}
        for _, region, _ in nodes:
            regions[region] = total([r for r in rows if r["region"] == region])
        cluster = total(rows) if rows else None
        if cluster is not None:
            for q, key in ((0.5, "p50_ms"), (0.95, "p95_ms"), (0.99, "p99_ms")):
                cluster[key] = self._merged_quantile(q, prev_buckets, cur_buckets)
            self._history.append({"ts": now, "regions": {r: v["txn_s"] for r, v in regions.items()}})
        with self._lock:
            self._status = {"cluster": cluster, "regions": regions, "nodes": per_node, "history": list(self._history)}

    @staticmethod
    def _merged_quantile(q: float, prev_sets, cur_sets) -> float | None:
        def merge(sets):
            acc: dict[float, float] = {}
            for s in sets:
                for le, n in s:
                    acc[le] = acc.get(le, 0.0) + n
            return sorted(acc.items())
        value = histogram_quantile(q, merge(prev_sets), merge(cur_sets))
        return None if value is None else round(value, 1)

    def status(self) -> dict:
        with self._lock:
            return self._status

    def start(self, nodes: Callable[[], list[tuple[str, str, str]]]) -> None:
        def loop():
            while not self._stop.wait(self._interval):
                try:
                    self.poll(nodes())
                except Exception:
                    log.exception("metrics poll failed")
        threading.Thread(target=loop, daemon=True, name="metrics").start()
