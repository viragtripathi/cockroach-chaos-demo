"""Guided showcase scenarios: requirements, steps with large captions, live results, reset."""
from __future__ import annotations

import logging
import statistics
import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from ..retry import classify, is_retryable, retry_with_backoff
from ..topology import Topology

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Step:
    label: str          # presenter button, e.g. "Take down us-east-1"
    caption: str        # big stage caption; {primary}, {far} etc. are filled from the run state
    action: Callable[["Context", dict], dict | None] | None = None
    observe: str = ""   # what the audience should look at


@dataclass(frozen=True)
class Scenario:
    id: str
    title: str
    tier: int
    summary: str
    steps: tuple[Step, ...]
    min_regions: int = 1
    min_nodes: int = 3
    needs_latency: bool = False
    needs_goal: str | None = None  # "region": the scenario switches the goal itself if needed
    min_primary_nodes: int = 0
    prepare: Callable[["Context", dict], None] | None = None
    reset: Callable[["Context", dict], None] | None = None

    def unmet(self, topo: Topology | None, latency: str | None) -> list[str]:
        if topo is None:
            return ["no cluster yet"]
        out = []
        if len(topo.regions) < self.min_regions:
            out.append(f"needs {self.min_regions}+ regions")
        if len(topo.nodes) < self.min_nodes:
            out.append(f"needs {self.min_nodes}+ nodes")
        if self.needs_latency and latency == "off":
            out.append("needs inter-region latency on")
        primary_nodes = sum(n.region == topo.primary.name for n in topo.nodes)
        if primary_nodes < self.min_primary_nodes:
            out.append(f"needs {self.min_primary_nodes} nodes in {topo.primary.name} (has {primary_nodes}): "
                       f"add a node there first")
        return out


class ScenarioError(ValueError):
    pass


class Context:
    """What scenario steps may touch. Thin helpers over the real collaborators."""

    def __init__(self, *, provisioner, chaos, elastic, workload, operations, events, connect: Callable,
                 lb_host: str, catalog=None, cdc=None, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.time):
        self.provisioner, self.chaos, self.elastic, self.workload = provisioner, chaos, elastic, workload
        self.operations, self.events, self.catalog, self.cdc = operations, events, catalog, cdc
        self._connect, self.lb_host, self.sleep, self.clock = connect, lb_host, sleep, clock

    @property
    def topo(self) -> Topology:
        topo = self.provisioner.topology
        if topo is None:
            raise ScenarioError("no cluster yet")
        return topo

    def connect(self, host: str | None = None, **kw):
        return self._connect(host or self.lb_host, dbname="chaos", **kw)

    def gateway(self, region: str, *, allow_isolated: bool = False) -> str:
        """A live node in `region`, addressed directly (bypasses the load balancer)."""
        down = self.chaos.down_nodes()
        if allow_isolated:
            down -= {name for name, f in self.chaos.flags().items() if f.get("isolated")}
        for n in self.topo.nodes:
            if n.region == region and n.name not in down:
                return n.ip
        raise ScenarioError(f"no live node in {region}")

    def _retry(self, fn, idempotent: bool):
        """Retry transient errors. Work that is not idempotent only retries 40001, which never commits."""
        retry_if = is_retryable if idempotent else (lambda e: classify(e) == "serialization")
        return retry_with_backoff(fn, max_retries=6, initial_backoff=0.2, retry_if=retry_if, sleep=self.sleep)

    def run(self, statements, host: str | None = None, *, idempotent: bool = True) -> None:
        """Run statements on one connection; the whole group is retried, so start groups with DROP ... IF EXISTS."""
        def once():
            conn = self.connect(host)
            try:
                with conn.cursor() as cur:
                    for sql in ([statements] if isinstance(statements, str) else statements):
                        cur.execute(sql)
            finally:
                conn.close()
        self._retry(once, idempotent)

    def query(self, sql: str, host: str | None = None, *, one: bool = False, idempotent: bool = True):
        def once():
            conn = self.connect(host)
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    return cur.fetchone() if one else cur.fetchall()
            finally:
                conn.close()
        return self._retry(once, idempotent)

    def timed(self, sql: str, host: str, repeat: int = 5) -> float:
        """Median wall-clock ms for `sql` on one connection to `host` (connection setup excluded)."""
        conn = self.connect(host)
        try:
            samples = []
            with conn.cursor() as cur:
                cur.execute(sql)  # warm up: plan cache, range descriptors
                for _ in range(repeat):
                    t0 = time.perf_counter()
                    cur.execute(sql)
                    if cur.description:
                        cur.fetchall()
                    samples.append((time.perf_counter() - t0) * 1000)
            return round(statistics.median(samples), 1)
        finally:
            conn.close()

    def wait_operation(self, timeout: float = 900) -> None:
        deadline = self.clock() + timeout
        while self.clock() < deadline:
            op = self.operations.snapshot()
            if not op or op["status"] != "running":
                if op and op["status"] == "failed":
                    raise ScenarioError(op["error"])
                return
            self.sleep(1)
        raise ScenarioError("the cluster operation did not finish in time")


class Runner:
    def __init__(self, scenarios: list[Scenario], ctx: Context, *,
                 run_in_background: Callable[[Callable[[], None]], None] | None = None):
        self._by_id = {s.id: s for s in scenarios}
        self._order = [s.id for s in scenarios]
        self._ctx = ctx
        self._run_bg = run_in_background or (lambda fn: threading.Thread(target=fn, daemon=True).start())
        self._lock = threading.Lock()
        self._active: Scenario | None = None
        self._state: dict = {}
        self._step = -1
        self._steps: list[dict] = []
        self._busy = False
        self._gen = 0  # bumped on every start and stop; work from an older run is discarded

    def catalog(self, topo, latency) -> list[dict]:
        return [{"id": s.id, "title": s.title, "tier": s.tier, "summary": s.summary, "steps": len(s.steps),
                 "unmet": s.unmet(topo, latency)} for s in (self._by_id[i] for i in self._order)]

    def status(self) -> dict | None:
        with self._lock:
            if self._active is None:
                return None
            s = self._active
            current = self._steps[self._step] if 0 <= self._step < len(self._steps) else None
            return {"id": s.id, "title": s.title, "tier": s.tier, "summary": s.summary, "step": self._step,
                    "busy": self._busy, "steps": [dict(x) for x in self._steps], "current": dict(current) if current else None,
                    "done": self._step >= len(s.steps) - 1 and not self._busy}

    def start(self, scenario_id: str, topo, latency) -> None:
        s = self._by_id.get(scenario_id)
        if s is None:
            raise ScenarioError(f"unknown scenario {scenario_id!r}")
        unmet = s.unmet(topo, latency)
        if unmet:
            raise ScenarioError(f"{s.title}: {', '.join(unmet)}")
        with self._lock:
            if self._busy:
                raise ScenarioError("a step is still running")
            previous, previous_state = self._active, self._state
            self._gen += 1
            gen = self._gen
            self._active, self._state, self._step = s, {}, -1
            self._steps = [{"label": st.label, "caption": st.caption, "observe": st.observe, "status": "pending",
                            "result": None, "error": None} for st in s.steps]
            state = self._state
            reset_previous = previous.reset if previous is not None and previous.reset else None
            self._busy = s.prepare is not None or reset_previous is not None

        def prep():
            try:
                if reset_previous:
                    reset_previous(self._ctx, previous_state)  # clean up the scenario this one replaces
                if s.prepare:
                    s.prepare(self._ctx, state)
                self._fill_captions(gen)
            except Exception as e:
                log.exception("scenario prepare failed")
                with self._lock:
                    if gen == self._gen:
                        self._steps[0]["error"] = f"setup failed: {e}"
            finally:
                with self._lock:
                    if gen == self._gen:
                        self._busy = False
        if self._busy:
            self._run_bg(prep)
        else:
            self._fill_captions(gen)
        self._ctx.events.publish(f"Scenario: {s.title}")

    def _fill_captions(self, gen: int) -> None:
        with self._lock:
            if gen != self._gen or self._active is None:
                return
            for st, spec in zip(self._steps, self._active.steps):
                try:
                    st["caption"] = spec.caption.format(**self._state)
                    st["label"] = spec.label.format(**self._state)
                except (KeyError, IndexError, ValueError):
                    pass

    def next(self) -> None:
        with self._lock:
            if self._active is None:
                raise ScenarioError("no scenario is running")
            if self._busy:
                raise ScenarioError("a step is still running")
            if self._step >= len(self._active.steps) - 1:
                raise ScenarioError("that was the last step")
            self._step += 1
            index, spec, gen, state = self._step, self._active.steps[self._step], self._gen, self._state
            self._steps[index]["status"] = "running"
            self._busy = True

        def work():
            result, error = None, None
            try:
                result = spec.action(self._ctx, state) if spec.action else None
            except Exception as e:
                log.exception("scenario step failed")
                error = str(e)
            self._fill_captions(gen)
            with self._lock:
                if gen != self._gen:
                    return  # the scenario was stopped or replaced while this step ran
                self._steps[index].update(status="failed" if error else "done", result=result, error=error)
                self._busy = False
        self._run_bg(work)

    def stop(self) -> None:
        with self._lock:
            s, state, self._active, self._busy = self._active, self._state, None, False
            self._gen += 1
        if s and s.reset:
            def work():
                try:
                    s.reset(self._ctx, state)
                except Exception:
                    log.exception("scenario reset failed")
            self._run_bg(work)
