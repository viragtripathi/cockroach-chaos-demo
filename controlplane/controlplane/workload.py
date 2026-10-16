"""Built-in "Payments" workload: transfers between accounts, an invariant that money is never lost, recovery time."""
from __future__ import annotations

import logging
import random
import threading
import time
import uuid
from collections import deque
from typing import Callable

from .retry import RETRYABLE, abandon_after, backoff_delay, classify, retry_with_backoff

log = logging.getLogger(__name__)

ACCOUNTS = 10_000
START_BALANCE = 1_000
TOTAL = ACCOUNTS * START_BALANCE
HISTORY_SECONDS = 120
MAX_RETRIES = 12
TRANSFER_TIMEOUT = 10.0  # client-side limit per attempt; COMMIT can hang past statement_timeout during an outage
CONN_MAX_AGE = 30.0  # like a pool's max lifetime: recovered nodes get traffic back within ~30 s


def baseline_before(history: list[dict], failure_at: float) -> float:
    before = [h["commits"] for h in history if failure_at - 10 <= h["ts"] < failure_at]
    return sum(before) / len(before) if before else 0.0


def recovery_time(history: list[dict], failure_at: float, baseline: float | None = None) -> float | None:
    """Seconds from the failure to the first 2 s window back at >=80% of the 10 s pre-failure average.
    0.0 when commits never dipped; None while still recovering or without a baseline."""
    if baseline is None:
        baseline = baseline_before(history, failure_at)
    if baseline <= 0:
        return None
    after = [h for h in history if h["ts"] >= int(failure_at)]
    for i in range(len(after) - 1):
        a, b = after[i], after[i + 1]
        if (a["commits"] + b["commits"]) / 2 >= 0.8 * baseline:
            return 0.0 if i == 0 else float(b["ts"] - failure_at)
    return None


class Stats:
    """Per-second counters for the stage chart, plus a rolling latency sample for percentiles."""

    def __init__(self, clock: Callable[[], float] = time.time):
        self._clock = clock
        self._lock = threading.Lock()
        self._seconds: dict[int, dict] = {}
        self._latencies: deque = deque(maxlen=20_000)
        self.errors_total: dict[str, int] = {}  # transient errors seen, by class (most were retried successfully)
        self.failed_total = 0  # transfers that gave up
        self.deduplicated = 0  # retries that found the transfer had already committed

    def _bucket(self, now: float) -> dict:
        sec = int(now)
        b = self._seconds.get(sec)
        if b is None:
            b = self._seconds[sec] = {"ts": sec, "commits": 0, "retries": 0, "errors": 0, "regions": {}}
            for old in [s for s in self._seconds if s < sec - HISTORY_SECONDS]:
                del self._seconds[old]
        return b

    def commit(self, ms: float, region: str | None, now: float | None = None) -> None:
        now = self._clock() if now is None else now
        with self._lock:
            b = self._bucket(now)
            b["commits"] += 1
            if region:
                b["regions"][region] = b["regions"].get(region, 0) + 1
            self._latencies.append((now, ms))

    def retry(self, now: float | None = None) -> None:
        with self._lock:
            self._bucket(self._clock() if now is None else now)["retries"] += 1

    def fail(self) -> None:
        with self._lock:
            self.failed_total += 1

    def dedup(self) -> None:
        with self._lock:
            self.deduplicated += 1

    def error(self, kind: str, now: float | None = None) -> None:
        with self._lock:
            self._bucket(self._clock() if now is None else now)["errors"] += 1
            self.errors_total[kind] = self.errors_total.get(kind, 0) + 1

    def window(self, now: float | None = None) -> dict:
        now = self._clock() if now is None else now
        with self._lock:
            first = min(self._seconds, default=int(now))
            empty = {"commits": 0, "retries": 0, "errors": 0, "regions": {}}
            done = []
            for sec in range(max(first, int(now) - HISTORY_SECONDS), int(now)):  # dense: a stall shows as zeros
                b = self._seconds.get(sec, {**empty, "ts": sec})
                done.append(dict(b, regions=dict(b["regions"])))
            recent = sorted(ms for ts, ms in self._latencies if ts >= now - 10)
            errors = dict(self.errors_total)

        def pct(q):
            return round(recent[min(len(recent) - 1, int(q * len(recent)))], 1) if recent else None
        return {"history": done[-HISTORY_SECONDS:], "p50_ms": pct(0.5), "p95_ms": pct(0.95), "p99_ms": pct(0.99),
                "errors_total": errors}


class Workload:
    def __init__(self, connect: Callable, *, host: str, events=None, clock: Callable[[], float] = time.time,
                 sleep: Callable[[float], None] = time.sleep):
        self._connect = connect
        self._host = host
        self._events = events
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self.workers = 48  # enough that throughput is paced by the rate, not by cross-region latency
        self.rate = 250
        self.stats = Stats(clock)
        self._balance: int | None = None
        self._last_check: float | None = None
        self._failure_at: float | None = None
        self._baseline: float | None = None
        self._recovery: float | None = None
        self._last_error: str | None = None
        self._ids: list[str] = []
        self.running = False

    # -- control --

    def configure(self, *, workers: int, rate: int) -> None:
        if not 1 <= workers <= 64 or not 1 <= rate <= 5000:
            raise ValueError("workers must be 1-64 and rate 1-5000 tx/s")
        restart = self.running
        if restart:
            self.stop()
        self.workers, self.rate = workers, rate
        if restart:
            self.start()

    def setup(self) -> None:
        retry_with_backoff(self._setup_once, max_retries=5, initial_backoff=0.5, sleep=self._sleep)

    def _setup_once(self) -> None:
        conn = self._connect(self._host, statement_timeout="120s")  # seeding 10k rows multi-region takes a while
        try:
            with conn.cursor() as cur:
                # Random UUID keys spread writes across ranges; sequential integers would hot-spot one range.
                cur.execute("SELECT data_type FROM information_schema.columns "
                            "WHERE table_name = 'accounts' AND column_name = 'id'")
                row = cur.fetchone()
                if row and str(row[0]).lower() != "uuid":
                    cur.execute("DROP TABLE accounts")  # created by an older version with integer keys
                cur.execute("CREATE TABLE IF NOT EXISTS accounts "
                            "(id UUID PRIMARY KEY DEFAULT gen_random_uuid(), balance INT NOT NULL)")
                # One row per transfer id makes retries after ambiguous commits safe; rows expire after an hour.
                cur.execute("CREATE TABLE IF NOT EXISTS transfers (id UUID PRIMARY KEY, src UUID NOT NULL, dst UUID NOT NULL, "
                            "amount INT NOT NULL, at TIMESTAMPTZ NOT NULL DEFAULT now()) WITH (ttl_expire_after = '1 hour')")
                cur.execute("SELECT count(*) FROM accounts")
                if cur.fetchone()[0] != ACCOUNTS:
                    cur.execute("TRUNCATE accounts")
                    cur.execute(f"INSERT INTO accounts (balance) SELECT {START_BALANCE} FROM generate_series(1, {ACCOUNTS})")
                cur.execute("SELECT id FROM accounts")
                self._ids = [str(r[0]) for r in cur.fetchall()]
        finally:
            conn.close()

    def start(self) -> None:
        with self._lock:
            if self.running:
                return
            self.setup()
            self._stop.clear()
            self.running = True
            self._threads = [threading.Thread(target=self._worker, args=(i,), daemon=True, name=f"payments-{i}")
                             for i in range(self.workers)]
            self._threads.append(threading.Thread(target=self._checker, daemon=True, name="payments-invariant"))
            for t in self._threads:
                t.start()

    def stop(self) -> None:
        with self._lock:
            self._stop.set()
            for t in self._threads:
                t.join(timeout=8)
            self._threads = []
            self.running = False
            self._failure_at = self._baseline = None  # a recovery can't be measured without traffic

    def mark_failure(self, at: float | None = None) -> None:
        if self.running:
            self._failure_at = self._clock() if at is None else at
            self._baseline = baseline_before(self.stats.window()["history"], self._failure_at)
            self._recovery = None

    # -- status --

    def status(self) -> dict:
        w = self.stats.window()
        history = w["history"]
        last = history[-1] if history else {"commits": 0, "retries": 0, "errors": 0}
        if self._failure_at is not None and self._recovery is None:
            self._recovery = recovery_time(history, self._failure_at, self._baseline)
        balance = self._balance
        return {
            "running": self.running, "workers": self.workers, "rate": self.rate,
            "tps": last["commits"], "retries_s": last["retries"], "errors_s": last["errors"],
            "p50_ms": w["p50_ms"], "p95_ms": w["p95_ms"], "p99_ms": w["p99_ms"], "errors_total": w["errors_total"],
            "failed_total": self.stats.failed_total, "deduplicated": self.stats.deduplicated,
            "balance": balance, "expected": TOTAL,
            "money_lost": None if balance is None else TOTAL - balance,
            "invariant_ok": None if balance is None else balance == TOTAL,
            "last_check": self._last_check, "failure_at": self._failure_at,
            "recovery_s": self._recovery, "recovering": self._failure_at is not None and self._recovery is None,
            "last_error": self._last_error,
            "history": [{"ts": h["ts"], "commits": h["commits"], "retries": h["retries"], "errors": h["errors"],
                         "regions": h["regions"]} for h in history[-HISTORY_SECONDS:]],
        }

    def check_invariant(self) -> None:
        retry_with_backoff(self._check_once, max_retries=3, initial_backoff=0.2, sleep=self._sleep)

    def _check_once(self) -> None:
        conn = self._connect(self._host, statement_timeout="20s")
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT sum(balance) FROM accounts")
                self._balance = int(cur.fetchone()[0] or 0)
                self._last_check = self._clock()
        finally:
            conn.close()

    # -- workers --

    def _transfer(self, conn, tid: str, a: str, b: str, amount: int) -> None:
        """One transfer, idempotent: the transfer id is written in the same transaction, so a retry after an
        ambiguous commit either applies it or fails with a duplicate key because it already landed."""
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("INSERT INTO transfers (id, src, dst, amount) VALUES (%s, %s, %s, %s)", (tid, a, b, amount))
                cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, a))
                cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, b))

    def _open(self):
        conn = self._connect(self._host)
        region = None
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT gateway_region()")
                region = cur.fetchone()[0]
        except Exception:
            pass  # single-region clusters have no gateway region
        return conn, region

    def _close(self, state: dict) -> None:
        conn, state["conn"] = state.get("conn"), None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    def _send(self, state: dict, a: str, b: str, amount: int, rng: random.Random) -> str:
        """Apply one transfer exactly once, retrying every transient error class.
        Returns committed | deduplicated | the error class that made it give up."""
        tid = str(uuid.UUID(int=rng.getrandbits(128), version=4))
        started = self._clock()
        kind = "gave-up"
        for attempt in range(MAX_RETRIES):
            if attempt:
                self._stop.wait(backoff_delay(attempt - 1, initial_backoff=0.05, max_backoff=3.0))
                if self._stop.is_set():
                    return "stopped"
            if state.get("conn") is None:
                try:
                    state["conn"], state["region"] = self._open()
                    state["opened"] = self._clock() - rng.random() * CONN_MAX_AGE / 2
                except Exception as e:
                    self.stats.error("connection")
                    self._last_error = f"connect: {e}"
                    kind = "connection"
                    continue
            try:
                with abandon_after(state["conn"], TRANSFER_TIMEOUT):
                    self._transfer(state["conn"], tid, a, b, amount)
                self.stats.commit((self._clock() - started) * 1000, state.get("region"))
                return "committed"
            except Exception as e:
                kind = classify(e)
                if kind == "duplicate" and attempt:  # an earlier attempt committed after all
                    self.stats.commit((self._clock() - started) * 1000, state.get("region"))
                    self.stats.dedup()
                    return "deduplicated"
                self._last_error = f"{kind}: {e}"
                if kind not in RETRYABLE:
                    self.stats.fail()
                    self._close(state)
                    return kind
                self.stats.retry()
                if kind != "serialization":  # the connection may be gone and the commit may have happened
                    self.stats.error(kind)
                    self._close(state)
        self.stats.fail()
        return kind

    def _worker(self, index: int) -> None:
        rng = random.Random(index * 7919 + int(self._clock()))
        state: dict = {"conn": None}
        interval = self.workers / self.rate
        next_at = self._clock() + rng.random() * interval
        while not self._stop.is_set():
            if state["conn"] is not None and self._clock() - state.get("opened", 0) > CONN_MAX_AGE:
                self._close(state)
            delay = next_at - self._clock()
            if delay > 0:
                self._stop.wait(delay)
            next_at = max(next_at + interval, self._clock() - interval)
            a, b = rng.sample(self._ids, 2)
            self._send(state, a, b, rng.randint(1, 100), rng)
        self._close(state)

    def _checker(self) -> None:
        while not self._stop.wait(5.0):
            try:
                self.check_invariant()
            except Exception as e:
                log.debug("invariant check failed: %s", e)
