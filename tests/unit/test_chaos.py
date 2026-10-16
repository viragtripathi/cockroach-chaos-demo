from pathlib import Path
from types import SimpleNamespace

import pytest

from controlplane.catalog import load_catalog
from controlplane.chaos import Chaos, ChaosError
from controlplane.docker_ops import ContainerInfo, DockerError
from controlplane.guard import predict_unavailable
from controlplane.netem import Brownout
from controlplane.store import Store
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
CAT = load_catalog(ROOT / "catalog" / "regions.yaml")
KW = dict(default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
AWS = load_preset(ROOT / "presets", "aws", CAT, **KW)
SMALL = load_preset(ROOT / "presets", "small", CAT, **KW)
EAST = [n.name for n in AWS.nodes if n.region == "us-east-1"]


class FakeDocker:
    def __init__(self, topo):
        self.state = {n.name: {"running": True, "attached": True} for n in topo.nodes}
        self.calls = []

    def list_nodes(self):
        return [ContainerInfo(name, name, s["running"], "running" if s["running"] else "exited", {},
                              {"crdb-chaos": "10.77.0.1"} if s["attached"] else {}) for name, s in self.state.items()]

    def kill(self, name, signal):
        self.calls.append(("kill", name))
        if not self.state[name]["running"]:
            raise DockerError("not running")
        self.state[name]["running"] = False

    def stop(self, name, grace=20):
        self.calls.append(("stop", name))
        self.state[name]["running"] = False

    def start(self, name):
        self.calls.append(("start", name))
        self.state[name]["running"] = True

    def network_disconnect(self, network, name):
        self.calls.append(("disconnect", name, network))
        self.state[name]["attached"] = False

    def network_connect(self, network, name, ip):
        self.calls.append(("connect", name, network, ip))
        self.state[name]["attached"] = True


class FakeNetem:
    def __init__(self, fail=False):
        self.applied, self.fail = [], fail

    def apply(self, node, topo, *, scale, brownout, isolated):
        if self.fail:
            raise DockerError("netem unavailable")
        self.applied.append((node.name, scale, brownout, isolated))


def make(tmp_path, topo=AWS, netem=None):
    docker = FakeDocker(topo)
    netem = netem or FakeNetem()
    chaos = Chaos(docker, SimpleNamespace(topology=topo), netem, Store(tmp_path), network="crdb-chaos")
    return chaos, docker, netem


def test_region_outage_kills_every_node_in_the_region(tmp_path):
    chaos, docker, _ = make(tmp_path)
    result = chaos.act("crash", "region", "us-east-1", confirm=True)
    assert [c[1] for c in docker.calls if c[0] == "kill"] == EAST
    assert result["ok"] is True and result["targets"] == EAST


def test_partition_and_heal_keep_the_static_ip_and_reapply_shaping(tmp_path):
    chaos, docker, netem = make(tmp_path)
    chaos.act("partition", "node", "us-east-1a-n1", confirm=True)
    assert ("disconnect", "us-east-1a-n1", "crdb-chaos") in docker.calls
    chaos.act("recover", "node", "us-east-1a-n1")
    assert ("connect", "us-east-1a-n1", "crdb-chaos", "10.77.1.11") in docker.calls
    assert netem.applied[-1][0] == "us-east-1a-n1"


def test_brownout_and_isolation_are_tracked_and_recover_clears_them(tmp_path):
    chaos, _, netem = make(tmp_path)
    chaos.act("brownout", "zone", "us-west-2a", delay_ms=500, loss_pct=1.0)
    assert chaos.flags_for("us-west-2a-n1")["brownout"] == {"delay_ms": 500, "jitter_ms": 166, "loss_pct": 1.0}
    assert netem.applied[-1][2] == Brownout(500, 166, 1.0)
    chaos.act("isolate", "region", "eu-west-1", confirm=True)
    assert chaos.flags_for("eu-west-1a-n1")["isolated"] is True
    chaos.recover_all()
    assert chaos.flags_for("us-west-2a-n1") == {"brownout": None, "isolated": False}
    assert chaos.flags_for("eu-west-1a-n1")["isolated"] is False


def test_recover_starts_stopped_nodes_and_reconnects_detached_ones(tmp_path):
    chaos, docker, _ = make(tmp_path)
    docker.state["us-east-1a-n1"]["running"] = False
    docker.state["us-east-1b-n2"]["attached"] = False
    chaos.act("recover", "region", "us-east-1")
    assert ("start", "us-east-1a-n1") in docker.calls
    assert ("connect", "us-east-1b-n2", "crdb-chaos", "10.77.1.12") in docker.calls


def test_unknown_targets_and_actions_are_rejected(tmp_path):
    chaos, _, _ = make(tmp_path)
    with pytest.raises(ChaosError, match="no node"):
        chaos.act("crash", "node", "mars-1a-n1")
    with pytest.raises(ChaosError, match="unknown action"):
        chaos.act("explode", "node", "us-east-1a-n1")
    with pytest.raises(ChaosError, match="no cluster"):
        Chaos(FakeDocker(AWS), SimpleNamespace(topology=None), FakeNetem(), Store(tmp_path), network="n").act("crash", "all", "")


def test_actions_that_exceed_the_survival_goal_need_confirmation(tmp_path):
    chaos, docker, _ = make(tmp_path)
    chaos.act("crash", "region", "us-east-1", confirm=True)
    result = chaos.act("crash", "region", "us-west-2")
    assert result["ok"] is False and "REGION survival" in result["warning"]
    assert not [c for c in docker.calls if c == ("kill", "us-west-2a-n1")]
    assert chaos.act("crash", "region", "us-west-2", confirm=True)["ok"] is True


def test_latency_setting_is_persisted_and_applied_to_every_running_node(tmp_path):
    chaos, docker, netem = make(tmp_path)
    docker.state["us-east-1a-n1"]["running"] = False
    chaos.set_latency("off")
    assert chaos.latency_setting() == "off"
    assert {a[0] for a in netem.applied} == {n.name for n in AWS.nodes} - {"us-east-1a-n1"}
    assert all(a[1] is None for a in netem.applied)
    assert Store(tmp_path).get("latency") == "off"


def test_shaping_failures_become_warnings_not_errors(tmp_path):
    chaos, _, _ = make(tmp_path, netem=FakeNetem(fail=True))
    result = chaos.act("brownout", "node", "us-east-1a-n1")
    assert result["ok"] is True and "netem unavailable" in result["warnings"][0]


@pytest.mark.parametrize("topo, down, warns", [
    (AWS, set(EAST), False),
    (AWS, set(EAST) | {"us-west-2a-n1"}, True),
    (AWS, {"us-east-1a-n1", "us-west-2a-n1"}, False),
    (AWS, set(EAST) | {"us-west-2a-n1", "us-west-2b-n2", "us-west-2c-n3"}, True),
    (SMALL, {"us-east-1a-n1"}, False),
    (SMALL, {"us-east-1a-n1", "us-west-2a-n1"}, True),
])
def test_survival_guard(topo, down, warns):
    assert (predict_unavailable(topo, down) is not None) is warns


def test_forget_drops_flags_for_removed_nodes(tmp_path):
    chaos, _, _ = make(tmp_path)
    chaos.act("brownout", "node", "us-east-1a-n1")
    chaos.forget(["us-east-1a-n1"])
    assert chaos.flags() == {}


def test_disruptive_actions_notify_listeners(tmp_path):
    chaos, _, _ = make(tmp_path)
    hits = []
    chaos.on_disrupt.append(lambda: hits.append(1))
    chaos.act("brownout", "node", "us-east-1a-n1")
    chaos.act("crash", "node", "us-east-1a-n1")
    assert hits == [1]


def test_latency_setting_must_be_off_realistic_or_a_sane_multiplier(tmp_path):
    from controlplane.chaos import ChaosError
    chaos, _, _ = make(tmp_path)
    for bad in ("fast", "0", "50"):
        with pytest.raises(ChaosError):
            chaos.set_latency(bad)
    chaos.set_latency("0.5")
    assert chaos.latency_setting() == "0.5"


def test_region_goal_guard_counts_partial_losses_across_regions():
    # two primary nodes plus one elsewhere can hold 3 of a range's 5 replicas
    assert predict_unavailable(AWS, {"us-east-1a-n1", "us-east-1b-n2", "eu-west-1a-n1"}) is not None
    assert predict_unavailable(AWS, {"us-east-1a-n1", "eu-west-1a-n1"}) is None


def test_disruptive_chaos_waits_for_running_operations(tmp_path):
    from controlplane.chaos import ChaosError
    chaos, docker, _ = make(tmp_path)
    chaos._busy = lambda: "decommission"
    with pytest.raises(ChaosError, match="decommission"):
        chaos.act("crash", "node", "us-east-1a-n1")
    chaos.act("recover", "node", "us-east-1a-n1")  # recovery is always allowed


def test_flags_are_readable_while_an_action_holds_the_lock(tmp_path):
    import threading
    chaos, _, _ = make(tmp_path)
    with chaos._lock:
        out = []
        t = threading.Thread(target=lambda: out.append(chaos.flags()))
        t.start()
        t.join(timeout=2)
        assert out == [{}]


def test_runtime_failures_become_chaos_errors(tmp_path):
    from controlplane.chaos import ChaosError
    chaos, docker, _ = make(tmp_path)
    def boom(*a, **k):
        raise DockerError("no such container")
    docker.kill = boom
    with pytest.raises(ChaosError, match="no such container"):
        chaos.act("crash", "node", "us-east-1a-n1")


def test_reapply_reshapes_only_the_named_nodes(tmp_path):
    chaos, _, netem = make(tmp_path)
    netem.applied.clear()
    chaos.reapply(["us-east-1a-n1"])
    assert [a[0] for a in netem.applied] == ["us-east-1a-n1"]


def test_zone_survival_guard_on_a_single_region():
    one = load_preset(ROOT / "presets", "aws", CAT, model="single-region", **KW)
    zone_a = {n.name for n in one.nodes if n.zone.endswith("a")}
    assert predict_unavailable(one, zone_a) is None                      # one zone down: fine
    assert predict_unavailable(one, {n.name for n in one.nodes[:2]}) is not None  # two of three: not fine
