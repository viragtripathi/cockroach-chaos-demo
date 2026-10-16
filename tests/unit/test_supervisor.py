from pathlib import Path

from controlplane.catalog import load_catalog
from controlplane.docker_ops import ContainerInfo
from controlplane.events import Events
from controlplane.supervisor import Supervisor
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
TOPO = load_preset(ROOT / "presets", "classic", load_catalog(ROOT / "catalog" / "regions.yaml"),
                   default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)


class FakeDocker:
    def __init__(self, exits):
        self.exits, self.started = dict(exits), []  # name -> exit code (None = running)

    def list_nodes(self):
        return [ContainerInfo(n.name, n.name, self.exits.get(n.name) is None,
                              "running" if self.exits.get(n.name) is None else "exited", {}, {},
                              exit_code=self.exits.get(n.name)) for n in TOPO.nodes]

    def start(self, name):
        self.started.append(name)
        self.exits[name] = None


class FakeChaos:
    def __init__(self):
        self.applied = []

    def reapply(self, names):
        self.applied.append(list(names))
        return []


def make(exits, busy=None, clock=None):
    docker, chaos, events = FakeDocker(exits), FakeChaos(), Events()
    prov = type("P", (), {"topology": TOPO})()
    sup = Supervisor(docker, prov, chaos, events, busy=busy or (lambda: None), clock=clock or (lambda: 0.0))
    return sup, docker, chaos, events


def test_nodes_that_died_on_a_fatal_error_are_restarted_and_reshaped():
    sup, docker, chaos, events = make({"us-west-2a-n1": 7, "us-east-1a-n1": 137, "us-east-1b-n2": 0})
    assert sup.tick() == ["us-west-2a-n1"]
    assert docker.started == ["us-west-2a-n1"]          # not the crashed (137) or stopped (0) ones
    assert chaos.applied == [["us-west-2a-n1"]]          # latency shaping comes back with it
    assert "us-west-2a-n1" in events.recent()[-1]["text"]


def test_nothing_is_restarted_while_the_cluster_is_being_rebuilt():
    sup, docker, _, _ = make({"us-west-2a-n1": 7}, busy=lambda: "rebuild")
    assert sup.tick() == [] and docker.started == []


def test_a_node_that_keeps_dying_is_backed_off_then_left_alone():
    now = [0.0]
    sup, docker, _, events = make({"us-west-2a-n1": 7}, clock=lambda: now[0])
    restarts = 0
    for step in range(40):
        now[0] = step * 15.0          # it dies again every 15 s
        docker.exits["us-west-2a-n1"] = 7
        restarts += len(sup.tick())
    assert 2 <= restarts <= 5           # at most once a minute, and it gives up after a few tries
    assert any("keeps stopping" in e["text"] for e in events.recent())
