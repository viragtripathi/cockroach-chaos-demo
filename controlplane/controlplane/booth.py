"""Booth mode and hardware triggers: map a trigger id to a chaos action, pick safe random victims, auto-heal."""
from __future__ import annotations

import logging
import random
import threading
import time

from .guard import predict_unavailable
from .topology import Topology

log = logging.getLogger(__name__)

TRIGGER_ACTIONS = {
    "crash": "crash", "stop": "stop", "partition": "partition", "brownout": "brownout", "isolate": "isolate",
    "zone-outage": "outage", "region-outage": "outage", "recover-all": "recover", "recover": "recover",
}
TRIGGER_TARGETS = ("node", "zone", "region", "all", "random-node", "random-region", "random-safe")


class TriggerError(ValueError):
    """Unknown trigger, bad mapping, or nothing left to hit."""


def _kind_for(action: str) -> str:
    return {"zone-outage": "zone", "region-outage": "region", "isolate": "region"}.get(action, "node")


def resolve_trigger(spec: dict, topo: Topology, down: set[str], rng: random.Random) -> tuple[str, str, str]:
    action, target = spec.get("action"), spec.get("target", "random-safe")
    if action not in TRIGGER_ACTIONS:
        raise TriggerError(f"unknown trigger action {action!r}")
    if target not in TRIGGER_TARGETS:
        raise TriggerError(f"unknown trigger target {target!r}")
    verb = TRIGGER_ACTIONS[action]
    if action == "recover-all" or target == "all":
        return verb, "all", ""
    if target in ("node", "zone", "region"):
        return verb, target, str(spec.get("value", ""))

    kind = _kind_for(action)
    groups: dict[str, list[str]] = {}
    for n in topo.nodes:
        groups.setdefault(getattr(n, "name" if kind == "node" else kind), []).append(n.name)
    up = {k: v for k, v in groups.items() if not all(name in down for name in v)}
    if target == "random-safe":
        up = {k: v for k, v in up.items() if predict_unavailable(topo, down | set(v)) is None}
    if not up:
        raise TriggerError("no safe target left; recover first" if target == "random-safe" else "everything is already down")
    return verb, kind, rng.choice(sorted(up))


class Booth:
    def __init__(self, chaos, store, events, *, rng: random.Random | None = None, clock=time.time, busy=None):
        self._chaos = chaos
        self._store = store
        self._events = events
        self._rng = rng or random.Random()
        self._clock = clock
        self._busy = busy or (lambda: None)
        self._stop = threading.Event()

    def settings(self) -> dict:
        return self._store.get("booth")

    def fire(self, trigger_id: str) -> dict:
        spec = self._store.get("triggers").get(trigger_id)
        if spec is None:
            raise TriggerError(f"unknown trigger {trigger_id!r}")
        booth = self.settings()["enabled"]
        topo = self._chaos.topology()
        down = self._chaos.down_nodes()
        target = spec.get("target")
        if booth and target in ("random-node", "random-region"):
            spec = {**spec, "target": "random-safe"}  # booth visitors never break the cluster
        try:
            action, kind, value = resolve_trigger(spec, topo, down, self._rng)
        except TriggerError as e:
            self._events.publish(f"Trigger {trigger_id}: {e}", "warn")
            return {"ok": False, "trigger": trigger_id, "message": str(e)}
        if booth and action != "recover":
            names = {n.name for n in topo.nodes if (value == "" or getattr(n, kind if kind != "node" else "name") == value)}
            warning = predict_unavailable(topo, down | names)
            if warning:
                self._events.publish(f"Trigger {trigger_id} refused in booth mode", "warn")
                return {"ok": False, "trigger": trigger_id, "action": action, "kind": kind, "value": value,
                        "message": f"Refused in booth mode: {warning}"}
        result = self._chaos.act(action, kind, value, confirm=True)
        label = "everything" if kind == "all" else value
        self._events.publish(f"{action.capitalize()} · {label}", "good" if action == "recover" else "bad")
        return {"ok": True, "trigger": trigger_id, "action": action, "kind": kind, "value": value,
                "targets": result.get("targets", []), "warnings": result.get("warnings", [])}

    def _dirty(self) -> bool:
        return bool(self._chaos.down_nodes() or self._chaos.flags())

    def restart_countdown(self) -> None:
        """Booth mode just turned on: give whatever is broken a full quiet period before healing it."""
        self._chaos.last_action_at = self._clock()

    def heal_in(self) -> int | None:
        s = self.settings()
        if not s["enabled"]:
            return None
        try:
            if not self._dirty():
                return None
        except Exception:  # no cluster yet
            return None
        if not self._chaos.last_action_at:  # broken before we were watching (e.g. after a restart)
            self._chaos.last_action_at = self._clock()
        return max(0, int(round(self._chaos.last_action_at + s["auto_heal_s"] - self._clock())))

    def status(self) -> dict:
        return {**self.settings(), "heal_in": self.heal_in(), "counters": self._store.get("counters")}

    def tick(self) -> None:
        if self._busy():
            return  # never heal in the middle of a rebuild or a topology change
        if self.heal_in() == 0:
            self._chaos.recover_all()
            self._chaos.last_action_at = 0.0
            self._events.publish("Auto-heal: recovering everything", "good")

    def start(self) -> None:
        def loop():
            while not self._stop.wait(1.0):
                try:
                    self.tick()
                except Exception:
                    log.exception("booth tick failed")
        threading.Thread(target=loop, daemon=True, name="booth").start()
