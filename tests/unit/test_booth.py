import random
from pathlib import Path

import pytest

from controlplane.booth import Booth, TriggerError, resolve_trigger
from controlplane.catalog import load_catalog
from controlplane.events import Events
from controlplane.store import Store
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
CAT = load_catalog(ROOT / "catalog" / "regions.yaml")
KW = dict(default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
AWS = load_preset(ROOT / "presets", "aws", CAT, **KW)
EAST = {n.name for n in AWS.nodes if n.region == "us-east-1"}


def test_explicit_targets_resolve_to_chaos_actions():
    rng = random.Random(1)
    assert resolve_trigger({"action": "crash", "target": "node", "value": "x"}, AWS, set(), rng) == ("crash", "node", "x")
    assert resolve_trigger({"action": "region-outage", "target": "region", "value": "eu-west-1"}, AWS, set(), rng) == ("outage", "region", "eu-west-1")
    assert resolve_trigger({"action": "zone-outage", "target": "zone", "value": "us-east-1a"}, AWS, set(), rng) == ("outage", "zone", "us-east-1a")
    assert resolve_trigger({"action": "recover-all", "target": "all"}, AWS, set(), rng) == ("recover", "all", "")


def test_random_safe_only_picks_nodes_the_survival_goal_tolerates():
    rng = random.Random(7)
    down = set(EAST)  # one region already down under REGION survival: nothing else is safe
    with pytest.raises(TriggerError, match="no safe"):
        resolve_trigger({"action": "crash", "target": "random-safe"}, AWS, down, rng)
    for _ in range(20):
        action, kind, value = resolve_trigger({"action": "crash", "target": "random-safe"}, AWS, set(), rng)
        assert kind == "node" and value in {n.name for n in AWS.nodes}


def test_random_region_picks_a_region_that_is_still_up():
    rng = random.Random(3)
    for _ in range(20):
        _, kind, value = resolve_trigger({"action": "region-outage", "target": "random-region"}, AWS, set(EAST), rng)
        assert kind == "region" and value != "us-east-1"


def test_unknown_trigger_actions_are_rejected():
    with pytest.raises(TriggerError):
        resolve_trigger({"action": "nuke", "target": "all"}, AWS, set(), random.Random())


class FakeChaos:
    def __init__(self):
        self.calls, self.last_action_at, self.down = [], 0.0, set()

    def topology(self):
        return AWS

    def down_nodes(self):
        return set(self.down)

    def flags(self):
        return {}

    def act(self, action, kind, value, *, confirm=False):
        self.calls.append((action, kind, value, confirm))
        return {"ok": True, "targets": [value], "warnings": []}

    def recover_all(self):
        self.calls.append(("recover", "all", "", False))
        return {"ok": True, "targets": [], "warnings": []}


def test_fire_maps_trigger_ids_and_refuses_unsafe_hits_in_booth_mode(tmp_path):
    store, chaos, events = Store(tmp_path), FakeChaos(), Events()
    booth = Booth(chaos, store, events, rng=random.Random(2), clock=lambda: 100.0)
    result = booth.fire("roach-1")
    assert result["action"] == "crash" and chaos.calls[-1][3] is True
    store.set("booth", {"enabled": True, "auto_heal_s": 45})
    store.set("triggers", {**store.get("triggers"), "bad": {"action": "region-outage", "target": "region", "value": "us-west-2"}})
    chaos.down = set(EAST)
    result = booth.fire("bad")
    assert result["ok"] is False and "refused" in result["message"].lower()
    with pytest.raises(TriggerError, match="unknown trigger"):
        booth.fire("nope")
    assert events.recent()[-1]["text"]


def test_auto_heal_recovers_after_the_quiet_period_only_in_booth_mode(tmp_path):
    now = [0.0]
    store, chaos = Store(tmp_path), FakeChaos()
    booth = Booth(chaos, store, Events(), clock=lambda: now[0])
    chaos.down, chaos.last_action_at = {"us-east-1a-n1"}, 10.0
    now[0] = 100.0
    booth.tick()
    assert chaos.calls == []  # booth mode off
    store.set("booth", {"enabled": True, "auto_heal_s": 45})
    now[0] = 30.0
    booth.tick()
    assert chaos.calls == [] and booth.status()["heal_in"] == 25
    now[0] = 56.0
    booth.tick()
    assert chaos.calls[-1][0] == "recover"


def test_events_keep_a_bounded_recent_list():
    ev = Events(limit=3, clock=lambda: 1.0)
    for i in range(5):
        ev.publish(f"e{i}", level="warn")
    assert [e["text"] for e in ev.recent()] == ["e2", "e3", "e4"] and ev.recent()[0]["level"] == "warn"


def test_auto_heal_waits_for_running_operations_and_counts_down_from_enable(tmp_path):
    now = [100.0]
    store, chaos = Store(tmp_path), FakeChaos()
    store.set("booth", {"enabled": True, "auto_heal_s": 45})
    busy = ["rebuild"]
    booth = Booth(chaos, store, Events(), clock=lambda: now[0], busy=lambda: busy[0])
    chaos.down, chaos.last_action_at = {"us-east-1a-n1"}, 1.0  # an old failure from before booth mode
    booth.restart_countdown()
    assert booth.heal_in() == 45
    now[0] = 200.0
    booth.tick()
    assert chaos.calls == []          # a rebuild is running: never heal in the middle of it
    busy[0] = None
    booth.tick()
    assert chaos.calls[-1][0] == "recover"


def test_failures_found_after_a_restart_start_the_countdown(tmp_path):
    now = [50.0]
    store, chaos = Store(tmp_path), FakeChaos()
    store.set("booth", {"enabled": True, "auto_heal_s": 45})
    booth = Booth(chaos, store, Events(), clock=lambda: now[0])
    chaos.down, chaos.last_action_at = {"us-east-1a-n1"}, 0.0
    assert booth.heal_in() == 45
