"""Failure injection and recovery for nodes, zones and regions."""
from __future__ import annotations

import logging
import threading
import time

from .docker_ops import DockerError
from .guard import predict_unavailable
from .netem import Brownout, latency_scale
from .topology import NodeSpec, Topology

log = logging.getLogger(__name__)

ACTIONS = ("crash", "outage", "stop", "start", "partition", "heal", "brownout", "isolate", "recover")
DISRUPTIVE = {"crash", "outage", "stop", "partition", "isolate"}
TARGET_KINDS = ("node", "zone", "region", "all")


class ChaosError(ValueError):
    """The request can't be carried out (no cluster, unknown target or action)."""


class Chaos:
    def __init__(self, docker, provisioner, netem, store, *, network: str, clock=time.time, busy=None):
        self._docker = docker
        self._provisioner = provisioner
        self._netem = netem
        self._store = store
        self._network = network
        self._clock = clock
        self._lock = threading.RLock()  # serializes actions (held through slow runtime calls)
        self._flag_lock = threading.Lock()  # guards the flags only, so the stage never waits on an action
        self._busy = busy or (lambda: None)  # name of a running cluster operation, if any
        self._brownouts: dict[str, Brownout] = {}
        self._isolated: set[str] = set()
        self.last_action_at = 0.0
        self.on_disrupt: list = []  # e.g. start the workload's recovery-time clock

    # -- queries --

    def topology(self) -> Topology:
        topo = self._provisioner.topology
        if topo is None:
            raise ChaosError("no cluster yet")
        return topo

    def targets(self, kind: str, value: str) -> list[NodeSpec]:
        topo = self.topology()
        if kind not in TARGET_KINDS:
            raise ChaosError(f"unknown target kind {kind!r}")
        field = {"node": "name", "zone": "zone", "region": "region"}.get(kind)
        nodes = list(topo.nodes) if kind == "all" else [n for n in topo.nodes if getattr(n, field) == value]
        if not nodes:
            raise ChaosError(f"no {kind} named {value!r}")
        return nodes

    def latency_setting(self) -> str:
        return self._store.get("latency") or self.topology().latency

    def flags_for(self, name: str) -> dict:
        b = self._brownouts.get(name)
        return {"brownout": None if b is None else {"delay_ms": b.delay_ms, "jitter_ms": b.jitter_ms, "loss_pct": b.loss_pct},
                "isolated": name in self._isolated}

    def flags(self) -> dict[str, dict]:
        with self._flag_lock:
            return {n: self.flags_for(n) for n in set(self._brownouts) | self._isolated}

    def down_nodes(self, containers=None) -> set[str]:
        containers = containers if containers is not None else self._containers()
        down = {name for name, c in containers.items() if not c.running or self._network not in c.ips}
        with self._flag_lock:
            return down | set(self._isolated)

    # -- commands --

    def act(self, action: str, kind: str, value: str, *, confirm: bool = False,
            delay_ms: int = 700, jitter_ms: int | None = None, loss_pct: float = 2.0) -> dict:
        with self._lock:
            if action not in ACTIONS:
                raise ChaosError(f"unknown action {action!r}")
            topo = self.topology()
            nodes = self.targets(kind, value)
            busy = self._busy()
            if busy and action not in ("recover", "start", "heal"):
                raise ChaosError(f"{busy} in progress; wait for it to finish (recover still works)")
            names = [n.name for n in nodes]
            containers = self._containers()
            if action in DISRUPTIVE:
                warning = predict_unavailable(topo, self.down_nodes(containers) | set(names))
                if warning and not confirm:
                    return {"ok": False, "warning": warning, "targets": names}
            if action == "brownout":
                brownout = Brownout(int(delay_ms), int(jitter_ms if jitter_ms is not None else delay_ms // 3), float(loss_pct))
                with self._flag_lock:
                    for n in nodes:
                        self._brownouts[n.name] = brownout
            try:
                warnings = getattr(self, f"_{'crash' if action == 'outage' else action}")(nodes, containers)
            except DockerError as e:
                raise ChaosError(f"{action} failed: {e}") from e
            self.last_action_at = self._clock()
            if action in ("crash", "outage"):
                counters = self._store.get("counters")
                counters["nodes_crashed"] += len(nodes)
                counters["regions_down"] += 1 if kind == "region" else 0
                self._store.set("counters", counters)
            if action in DISRUPTIVE:
                for hook in self.on_disrupt:
                    hook()
            return {"ok": True, "targets": names, "warnings": warnings}

    def recover_all(self) -> dict:
        return self.act("recover", "all", "")

    def set_latency(self, setting: str) -> list[str]:
        if setting not in ("off", "realistic"):
            try:
                ok = 0.1 <= float(setting) <= 5
            except ValueError:
                ok = False
            if not ok:
                raise ChaosError("latency must be off, realistic, or a multiplier between 0.1 and 5")
        with self._lock:
            self._store.set("latency", setting)
            return self.apply_all()

    def apply_all(self) -> list[str]:
        """Re-apply shaping on every reachable node (after provisioning, adoption or a latency change)."""
        with self._lock:
            return self._apply(list(self.topology().nodes))

    def forget(self, names) -> None:
        with self._flag_lock:
            for name in names:
                self._brownouts.pop(name, None)
                self._isolated.discard(name)

    def reapply(self, names) -> list[str]:
        """Re-apply network shaping to nodes that just came back (e.g. restarted by the supervisor)."""
        with self._lock:
            wanted = set(names)
            return self._apply([n for n in self.topology().nodes if n.name in wanted])

    def reset(self) -> None:
        with self._flag_lock:
            self._brownouts.clear()
            self._isolated.clear()

    # -- action implementations --

    def _crash(self, nodes, containers):
        for n in nodes:
            if containers.get(n.name) and containers[n.name].running:
                self._docker.kill(n.name, "KILL")
        return []

    def _stop(self, nodes, containers):
        for n in nodes:
            if containers.get(n.name) and containers[n.name].running:
                self._docker.stop(n.name)
        return []

    def _start(self, nodes, containers):
        for n in nodes:
            if containers.get(n.name) and not containers[n.name].running:
                self._docker.start(n.name)
        return self._apply(nodes)

    def _partition(self, nodes, containers):
        for n in nodes:
            c = containers.get(n.name)
            if c and self._network in c.ips:
                self._docker.network_disconnect(self._network, n.name)
        return []

    def _heal(self, nodes, containers):
        for n in nodes:
            c = containers.get(n.name)
            if c and self._network not in c.ips:
                self._docker.network_connect(self._network, n.name, n.ip)
        return self._apply(nodes)

    def _brownout(self, nodes, containers):
        return self._apply(nodes)

    def _isolate(self, nodes, containers):
        with self._flag_lock:
            self._isolated.update(n.name for n in nodes)
        return self._apply(nodes)

    def _recover(self, nodes, containers):
        with self._flag_lock:
            for n in nodes:
                self._brownouts.pop(n.name, None)
                self._isolated.discard(n.name)
        for n in nodes:
            c = containers.get(n.name)
            if c and not c.running:
                self._docker.start(n.name)
            if c and self._network not in c.ips:
                self._docker.network_connect(self._network, n.name, n.ip)
        return self._apply(nodes)

    # -- helpers --

    def _containers(self) -> dict:
        return {c.name: c for c in self._docker.list_nodes()}

    def _apply(self, nodes) -> list[str]:
        topo = self.topology()
        scale = latency_scale(self.latency_setting())
        containers = self._containers()
        warnings = []
        for n in nodes:
            c = containers.get(n.name)
            if not c or not c.running or self._network not in c.ips:
                continue  # stopped or partitioned: shaping is re-applied when it comes back
            try:
                with self._flag_lock:
                    brownout, isolated = self._brownouts.get(n.name), n.name in self._isolated
                self._netem.apply(n, topo, scale=scale, brownout=brownout, isolated=isolated)
            except DockerError as e:
                log.warning("network shaping on %s failed: %s", n.name, e)
                warnings.append(f"network shaping on {n.name} failed: {e}")
        return warnings
