import subprocess
from pathlib import Path

import pytest

from controlplane.catalog import load_catalog
from controlplane.docker_ops import ContainerInfo, DockerError
from controlplane.operations import OperationBusy, Operations
from controlplane.provisioner import Provisioner
from controlplane.settings import Settings
from controlplane.state import load_state, save_state, topology_to_dict
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
CAT = load_catalog(ROOT / "catalog" / "regions.yaml")
KW = dict(default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
CLASSIC = load_preset(ROOT / "presets", "classic", CAT, **KW)
SMALL = load_preset(ROOT / "presets", "small", CAT, **KW)


class FakeDocker:
    def __init__(self, existing=(), image=True, pull_error=None, init_results=None, remove_fail_after=None):
        self.existing = list(existing)
        self.remove_fail_after = remove_fail_after
        self.started = []
        self.image = image
        self.pull_error = pull_error
        self.init_results = list(init_results or [(0, "Cluster successfully initialized", "")])
        self.pulled, self.ran, self.removed, self.execs = [], [], [], []

    def list_nodes(self):
        return list(self.existing)

    def image_present(self, image):
        return self.image

    def pull(self, image):
        if self.pull_error:
            raise DockerError(self.pull_error)
        self.pulled.append(image)

    def run_node(self, spec, topo, join, network):
        self.ran.append((spec.name, tuple(join), network))
        self.existing.append(ContainerInfo(spec.name, spec.name, True, "running", {}, {}))

    def remove(self, name):
        if self.remove_fail_after is not None and len(self.removed) >= self.remove_fail_after:
            raise DockerError("interrupted")
        self.removed.append(name)
        self.existing = [c for c in self.existing if c.name != name]

    def start(self, name):
        self.started.append(name)

    def exec(self, name, cmd, timeout=60.0, check=True):
        self.execs.append((name, list(cmd)))
        code, out, err = self.init_results.pop(0) if len(self.init_results) > 1 else self.init_results[0]
        return subprocess.CompletedProcess(cmd, code, out, err)


class FakeLB:
    def __init__(self):
        self.applied = []

    def apply(self, nodes):
        self.applied.append([n.name for n in nodes])

    def ensure_config(self):
        pass


class FakeCursor:
    def __init__(self, db):
        self.db = db

    def execute(self, sql):
        self.db.executed.append(sql)
        self._last = sql

    def fetchall(self):
        if "SHOW REGIONS FROM CLUSTER" in self._last:
            self.db.region_polls += 1
            return [(r,) for r in self.db.regions_by_poll(self.db.region_polls)]
        return []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeDB:
    def __init__(self, regions_by_poll):
        self.executed, self.region_polls, self.regions_by_poll = [], 0, regions_by_poll

    def connect(self, host, **kw):
        db = self

        class Conn:
            def cursor(self):
                return FakeCursor(db)

            def close(self):
                pass

        return Conn()


def make(tmp_path, docker, topo=CLASSIC, regions=None):
    settings = Settings.from_env({"DATA_DIR": str(tmp_path)})
    db = FakeDB(regions or (lambda poll: [r.name for r in topo.regions]))
    p = Provisioner(docker, FakeLB(), Operations(), settings, connect=db.connect, sleep=lambda s: None)
    return p, db


def test_provision_creates_every_node_inits_bootstraps_and_routes(tmp_path):
    docker = FakeDocker(image=False)
    p, db = make(tmp_path, docker)
    p.provision(CLASSIC)
    op = p._ops.snapshot()
    assert op["status"] == "done", op
    assert docker.pulled == ["docker.io/cockroachdb/cockroach:v26.3.2"]
    assert [r[0] for r in docker.ran] == [n.name for n in CLASSIC.nodes]
    assert docker.ran[0][1] == ("us-east-1a-n1:26257", "us-east-1b-n2:26257", "us-west-2a-n1:26257")
    init = docker.execs[0][1]
    assert init[:2] == ["/cockroach/cockroach", "init"] and "--cluster-name=global-payments" in init
    assert 'ALTER DATABASE defaultdb SET PRIMARY REGION "us-east-1"' in db.executed
    assert p._lb.applied[-1] == [n.name for n in CLASSIC.nodes]
    assert load_state(tmp_path)["provisioned"] is True
    assert "Allocating capacity in us-east-1" in op["steps"]
    assert not [s for s in op["steps"] if any(w in s.lower() for w in ("docker", "container", "podman", "localhost"))]


def test_init_retries_and_treats_already_initialized_as_success(tmp_path):
    docker = FakeDocker(init_results=[(1, "", "connection refused"), (1, "", "connection refused"),
                                      (1, "", "ERROR: cluster has already been initialized")])
    p, _ = make(tmp_path, docker)
    p.provision(CLASSIC)
    assert p._ops.snapshot()["status"] == "done"
    assert len(docker.execs) == 3


def test_pull_failure_fails_the_operation_with_a_clear_message(tmp_path):
    p, _ = make(tmp_path, FakeDocker(image=False, pull_error="manifest unknown"))
    p.provision(CLASSIC)
    op = p._ops.snapshot()
    assert op["status"] == "failed"
    assert op["error"].startswith("Could not pull image docker.io/cockroachdb/cockroach:v26.3.2")


def test_provisioning_waits_for_every_region_to_join(tmp_path):
    names = [r.name for r in CLASSIC.regions]
    p, db = make(tmp_path, FakeDocker(), regions=lambda poll: names[:2] if poll < 3 else names)
    p.provision(CLASSIC)
    assert p._ops.snapshot()["status"] == "done" and db.region_polls == 3


def test_regions_that_never_join_fail_with_their_names(tmp_path):
    p, _ = make(tmp_path, FakeDocker(), regions=lambda poll: ["us-east-1"])
    p.provision(CLASSIC)
    op = p._ops.snapshot()
    assert op["status"] == "failed" and "us-west-2" in op["error"] and "us-east-2" in op["error"]


def test_ensure_adopts_a_finished_cluster_without_recreating_nodes(tmp_path):
    existing = [ContainerInfo(n.name, n.name, True, "running", {}, {}) for n in CLASSIC.nodes]
    docker = FakeDocker(existing=existing)
    save_state(tmp_path, {"topology": topology_to_dict(CLASSIC), "provisioned": True})
    p, _ = make(tmp_path, docker)
    p.ensure(SMALL)
    assert docker.ran == [] and docker.removed == []
    assert p.topology == CLASSIC
    assert p._lb.applied == [[n.name for n in CLASSIC.nodes]]
    assert p._ops.snapshot()["kind"] == "adopt"


def test_ensure_rebuilds_a_half_built_cluster_instead_of_adopting_it(tmp_path):
    docker = FakeDocker(existing=[ContainerInfo("us-east-1a-n1", "x", True, "running", {}, {})])
    save_state(tmp_path, {"topology": topology_to_dict(CLASSIC), "provisioned": False})
    p, _ = make(tmp_path, docker, topo=SMALL)
    p.ensure(SMALL)
    assert docker.removed == ["us-east-1a-n1"]
    assert [r[0] for r in docker.ran] == [n.name for n in SMALL.nodes]
    assert p._ops.snapshot()["status"] == "done"


def test_ensure_provisions_when_nothing_exists(tmp_path):
    docker = FakeDocker()
    p, _ = make(tmp_path, docker, topo=SMALL)
    p.ensure(SMALL)
    assert [r[0] for r in docker.ran] == [n.name for n in SMALL.nodes]


def test_rebuild_drains_the_load_balancer_before_removing_nodes(tmp_path):
    existing = [ContainerInfo(n.name, n.name, True, "running", {}, {}) for n in CLASSIC.nodes]
    docker = FakeDocker(existing=existing)
    p, _ = make(tmp_path, docker, topo=SMALL)
    p.rebuild(SMALL)
    assert p._lb.applied[0] == []
    assert sorted(docker.removed) == sorted(n.name for n in CLASSIC.nodes)
    assert p._lb.applied[-1] == [n.name for n in SMALL.nodes]
    assert p.topology == SMALL


def test_only_one_operation_runs_at_a_time():
    ops = Operations()
    op = ops.start("provision")
    with pytest.raises(OperationBusy):
        ops.start("rebuild")
    ops.finish(op)
    assert ops.start("rebuild").kind == "rebuild"


def test_restart_mid_rebuild_does_not_adopt_a_half_deleted_cluster(tmp_path):
    existing = [ContainerInfo(n.name, n.name, True, "running", {}, {}) for n in CLASSIC.nodes]
    save_state(tmp_path, {"topology": topology_to_dict(CLASSIC), "provisioned": True})
    docker = FakeDocker(existing=existing, remove_fail_after=1)
    p, _ = make(tmp_path, docker, topo=SMALL)
    p.rebuild(SMALL)
    assert p._ops.snapshot()["status"] == "failed"
    docker.remove_fail_after = None
    fresh, _ = make(tmp_path, docker, topo=SMALL)
    fresh.ensure(SMALL)
    assert fresh._ops.snapshot()["kind"] == "provision"
    assert [r[0] for r in docker.ran] == [n.name for n in SMALL.nodes]


def test_adopt_starts_nodes_that_stopped_with_the_host(tmp_path):
    existing = [ContainerInfo(n.name, n.name, i != 0, "exited" if i == 0 else "running", {}, {}) for i, n in enumerate(CLASSIC.nodes)]
    save_state(tmp_path, {"topology": topology_to_dict(CLASSIC), "provisioned": True})
    docker = FakeDocker(existing=existing)
    p, _ = make(tmp_path, docker)
    p.ensure(CLASSIC)
    assert docker.started == [CLASSIC.nodes[0].name]
    assert p._ops.snapshot()["status"] == "done"


def test_ready_hooks_run_after_provision_and_adopt_and_teardown_hooks_before_rebuild(tmp_path):
    events = []
    docker = FakeDocker()
    p, _ = make(tmp_path, docker, topo=SMALL)
    p.on_ready.append(lambda: events.append("ready"))
    p.on_teardown.append(lambda: events.append("teardown"))
    p.provision(SMALL)
    p.rebuild(SMALL)
    assert events == ["ready", "teardown", "ready"]
    fresh, _ = make(tmp_path, docker, topo=SMALL)
    fresh.on_ready.append(lambda: events.append("adopted"))
    fresh.ensure(SMALL)
    assert events[-1] == "adopted"


def test_a_failing_ready_hook_is_reported_as_a_note_not_a_failure(tmp_path):
    p, _ = make(tmp_path, FakeDocker(), topo=SMALL)
    p.on_ready.append(lambda: (_ for _ in ()).throw(RuntimeError("tc missing")))
    p.provision(SMALL)
    op = p._ops.snapshot()
    assert op["status"] == "done" and any("tc missing" in s for s in op["steps"])


def test_commit_persists_a_changed_topology(tmp_path):
    p, _ = make(tmp_path, FakeDocker(), topo=SMALL)
    p.provision(SMALL)
    p.commit(CLASSIC)
    assert p.topology == CLASSIC and load_state(tmp_path)["topology"]["preset"] == "classic"


def test_progress_updates_the_last_step_in_place():
    ops = Operations()
    op = ops.start("decommission")
    ops.step(op, "Decommissioning n4")
    ops.progress(op, "Moving replicas off n4: 25 left")
    ops.progress(op, "Moving replicas off n4: 3 left")
    ops.step(op, "Releasing capacity")
    assert op.steps == ["Decommissioning n4", "Moving replicas off n4: 3 left", "Releasing capacity"]


def test_adopt_saved_reconnects_only_to_a_finished_cluster(tmp_path):
    existing = [ContainerInfo(n.name, n.name, True, "running", {}, {}) for n in CLASSIC.nodes]
    p, _ = make(tmp_path, FakeDocker(existing=existing))
    assert p.adopt_saved() is False  # nothing recorded yet
    save_state(tmp_path, {"topology": topology_to_dict(CLASSIC), "provisioned": True})
    assert p.adopt_saved() is True and p.topology == CLASSIC
