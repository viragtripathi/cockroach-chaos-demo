"""Bring back nodes that stopped on a CockroachDB fatal error, so a laptop hiccup doesn't leave the demo broken.

On a laptop every node shares one VM clock. When the Mac sleeps, the VM freezes; on wake its time service steps
the clock forward by minutes and every node sees its peers as "minutes away" and exits on purpose (clock
synchronization error, exit code 7). The same exit code covers disk stalls. Restarting is safe once the clock is
consistent again. Chaos actions never produce exit code 7 (crash is SIGKILL = 137, planned stop exits cleanly), so
the supervisor never undoes something the presenter did.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable

log = logging.getLogger(__name__)

FATAL_EXIT = 7          # cockroach exits 7 after log.Fatal (clock sync error, disk stall, ...)
MIN_INTERVAL = 60.0     # at most one automatic restart per node per minute
WINDOW, MAX_IN_WINDOW = 600.0, 4  # a node that needs more than this is left down for a while


class Supervisor:
    def __init__(self, docker, provisioner, chaos, events, *, busy: Callable[[], str | None] = lambda: None,
                 clock: Callable[[], float] = time.time, interval: float = 5.0):
        self._docker = docker
        self._provisioner = provisioner
        self._chaos = chaos
        self._events = events
        self._busy = busy
        self._clock = clock
        self._interval = interval
        self._restarts: dict[str, list[float]] = {}
        self._gave_up: set[str] = set()
        self._stop = threading.Event()

    def tick(self) -> list[str]:
        topo = self._provisioner.topology
        if topo is None or self._busy():
            return []  # a rebuild or topology change owns the nodes right now
        names = {n.name for n in topo.nodes}
        now = self._clock()
        restarted = []
        for c in self._docker.list_nodes():
            if c.name not in names or c.running or c.exit_code != FATAL_EXIT:
                continue
            history = [t for t in self._restarts.get(c.name, []) if now - t < WINDOW]
            self._restarts[c.name] = history
            if history and now - history[-1] < MIN_INTERVAL:
                continue
            if len(history) >= MAX_IN_WINDOW:
                if c.name not in self._gave_up:
                    self._gave_up.add(c.name)
                    self._events.publish(f"{c.name} keeps stopping on a fatal error; pausing automatic restarts "
                                         "for 10 minutes (check its logs)", "warn")
                continue
            self._gave_up.discard(c.name)
            try:
                self._docker.start(c.name)
            except Exception as e:
                log.warning("could not restart %s: %s", c.name, e)
                continue
            history.append(now)
            restarted.append(c.name)
            self._events.publish(f"{c.name} restarted after it stopped on a fatal error (e.g. a host clock jump)", "good")
        if restarted:
            for warning in self._chaos.reapply(restarted):
                log.warning(warning)
        return restarted

    def start(self) -> None:
        def loop():
            while not self._stop.wait(self._interval):
                try:
                    self.tick()
                except Exception:
                    log.exception("supervisor tick failed")
        threading.Thread(target=loop, daemon=True, name="supervisor").start()
