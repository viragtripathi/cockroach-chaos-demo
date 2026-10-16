import subprocess
from pathlib import Path

import pytest

from controlplane.catalog import load_catalog
from controlplane.docker_ops import ContainerInfo
from controlplane.elastic import Elastic, ElasticError
from controlplane.operations import Operations
from controlplane.settings import Settings
from controlplane.topology import load_preset, parse_preset

ROOT = Path(__file__).resolve().parents[2]
CAT = load_catalog(ROOT / "catalog" / "regions.yaml")
KW = dict(default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
CLASSIC = load_preset(ROOT / "presets", "classic", CAT, **KW)
AWS = load_preset(ROOT / "presets", "aws", CAT, **KW)
NINE = {"display_name": "Nine", "regions": [
    {"name": "us-east-1", "provider": "aws", "nodes": 3, "primary": True},
    {"name": "us-west-2", "provider": "aws", "nodes": 3, "secondary": True},
    {"name": "eu-west-1", "provider": "aws", "nodes": 3}]}
AWS9 = parse_preset("aws9", NINE, CAT, **KW)  # 3 nodes per region: room for ZONE survival


class FakeDocker:
    def __init__(self, topo, decommission_rc=0):
        self.containers = {n.name: ContainerInfo(n.name, n.name, True, "running", {}, {"crdb-chaos": n.ip}) for n in topo.nodes}
        self.ran, self.removed, self.execs, self.stopped = [], [], [], []
        self.decommission_rc = decommission_rc

    def list_nodes(self):
        return list(self.containers.values())

    def run_node(self, spec, topo, join, network):
        self.ran.append((spec.name, tuple(join)))
        self.containers[spec.name] = ContainerInfo(spec.name, spec.name, True, "running", {}, {network: spec.ip})

    def exec(self, name, cmd, timeout=60.0, check=True):
        self.execs.append((name, list(cmd)))
        return subprocess.CompletedProcess(cmd, self.decommission_rc, "", "" if not self.decommission_rc else "ERROR: not enough nodes")

    def stop(self, name, grace=20):
        self.stopped.append(name)

    def remove(self, name):
        self.removed.append(name)
        self.containers.pop(name, None)


class FakeDB:
    """Answers the handful of queries Elastic makes; records everything."""
    def __init__(self, topo, drain_polls=2):
        self.sql, self.topo, self.drain_polls, self.polls = [], topo, drain_polls, 0
        self.join_fails, self.fail_on, self.fail_once = False, None, None

    def connect(self, host, **kw):
        db = self

        class Cur:
            def execute(self, sql, params=None):
                if db.fail_on and db.fail_on in sql:
                    raise RuntimeError("region has rows homed there")
                if db.fail_once and db.fail_once in sql:
                    db.fail_once = None
                    err = RuntimeError("restart transaction: TransactionRetryWithProtoRefreshError")
                    err.sqlstate = "40001"
                    raise err
                db.sql.append(sql)
                self.last = sql

            def fetchone(self):
                if "sum(ranges)" in self.last:
                    db.polls += 1
                    return (0 if db.polls >= db.drain_polls else 40,)
                if "count(*)" in self.last:
                    return (0 if db.join_fails else 1,)
                return None

            def fetchall(self):
                if "SHOW REGIONS FROM CLUSTER" in self.last:
                    return [(r,) for r in db.regions]
                if "gossip_nodes" in self.last:
                    return [(i + 1, f"{n.name}:26257") for i, n in enumerate(db.all_nodes)]
                return []

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        class Conn:
            def cursor(self):
                return Cur()

            def close(self):
                pass

        return Conn()


class FakeProvisioner:
    def __init__(self, topo):
        self.topology = topo
        self.committed = []

    def commit(self, topo):
        self.topology = topo
        self.committed.append(topo)


class FakeLB:
    def __init__(self):
        self.applied = []

    def apply(self, nodes):
        self.applied.append([n.name for n in nodes])


class FakeChaos:
    def __init__(self):
        self.applied, self.forgotten = 0, []

    def apply_all(self):
        self.applied += 1
        return []

    def forget(self, names):
        self.forgotten.extend(names)


def make(tmp_path, topo=CLASSIC, **docker_kw):
    docker = FakeDocker(topo, **docker_kw)
    db = FakeDB(topo)
    db.regions = [r.name for r in topo.regions]
    db.all_nodes = list(topo.nodes)
    prov, lb, chaos, ops = FakeProvisioner(topo), FakeLB(), FakeChaos(), Operations()
    settings = Settings.from_env({"DATA_DIR": str(tmp_path)})
    e = Elastic(docker, prov, lb, ops, chaos, settings, connect=db.connect, catalog=CAT,
                sleep=lambda s: None, run_in_background=lambda f: f())
    return e, docker, db, prov, lb, chaos, ops


def test_add_node_runs_it_waits_for_it_and_routes_to_it(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path)
    db.all_nodes = list(CLASSIC.nodes) + []  # the new node shows up in gossip below
    e.add_node("us-east-2")
    assert ops.snapshot()["status"] == "done", ops.snapshot()
    assert docker.ran[0][0] == "us-east-2b-n2"
    assert docker.ran[0][1] == tuple(f"{n.name}:26257" for n in CLASSIC.nodes[:3])
    assert prov.topology.nodes[-1].name == "us-east-2b-n2"
    assert "us-east-2b-n2" in lb.applied[-1] and chaos.applied == 1


def test_add_node_validation_errors_are_immediate(tmp_path):
    e, *_ = make(tmp_path)
    with pytest.raises(ElasticError, match="no region"):
        e.add_node("mars-1")


def test_decommission_drains_then_removes_and_reroutes(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path)
    e.decommission("us-west-2b-n2")
    op = ops.snapshot()
    assert op["status"] == "done", op
    cmds = [c for _, c in docker.execs]
    assert cmds[0][:3] == ["/cockroach/cockroach", "node", "decommission"] and "4" in cmds[0] and "--wait=none" in cmds[0]
    assert "--cluster-name=global-payments" in cmds[0]
    assert docker.execs[0][0] != "us-west-2b-n2"  # run from another node
    assert docker.removed == ["us-west-2b-n2"] and "us-west-2b-n2" not in lb.applied[-1]
    assert all(n.name != "us-west-2b-n2" for n in prov.topology.nodes)
    assert chaos.forgotten == ["us-west-2b-n2"]
    assert any("replicas" in s.lower() for s in op["steps"])


def test_decommission_refuses_the_last_node_of_a_region(tmp_path):
    e, *_ = make(tmp_path)
    with pytest.raises(ElasticError, match="last node"):
        e.decommission("us-east-2a-n1")


def test_decommission_surfaces_cockroach_errors(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path, decommission_rc=1)
    e.decommission("us-west-2b-n2")
    op = ops.snapshot()
    assert op["status"] == "failed" and "not enough nodes" in op["error"]
    assert docker.removed == [] and prov.committed == []


def test_add_region_creates_nodes_and_adds_it_to_both_databases(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path)
    db.regions = [r.name for r in CLASSIC.regions] + ["eu-west-1"]
    e.add_region("eu-west-1", "aws", 3)
    assert ops.snapshot()["status"] == "done", ops.snapshot()
    assert len(docker.ran) == 3
    assert any("system.locations" in s and "eu-west-1" in s for s in db.sql)
    for d in ("defaultdb", "chaos"):
        assert f'ALTER DATABASE {d} ADD REGION IF NOT EXISTS "eu-west-1"' in db.sql
    assert prov.topology.regions[-1].name == "eu-west-1" and chaos.applied == 1


def test_remove_secondary_region_drops_secondary_then_region_then_decommissions(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path, topo=AWS)
    e.remove_region("us-west-2")
    op = ops.snapshot()
    assert op["status"] == "done", op
    joined = "\n".join(db.sql)
    # AWS has 3 regions under REGION survival; 2 left can only survive zone failures
    assert joined.index("DROP SECONDARY REGION") < joined.index("SURVIVE ZONE FAILURE") < joined.index('DROP REGION IF EXISTS "us-west-2"')
    assert sorted(docker.removed) == sorted(n.name for n in AWS.nodes if n.region == "us-west-2")
    assert prov.topology.survival_goal == "zone" and [r.name for r in prov.topology.regions] == ["us-east-1", "eu-west-1"]


def test_remove_primary_region_is_refused(tmp_path):
    e, *_ = make(tmp_path)
    with pytest.raises(ElasticError, match="primary"):
        e.remove_region("us-east-1")


def test_survival_goal_change_runs_on_both_databases(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path, topo=AWS9)
    e.set_survival("zone")
    assert ops.snapshot()["status"] == "done"
    assert "ALTER DATABASE defaultdb SURVIVE ZONE FAILURE" in db.sql and "ALTER DATABASE chaos SURVIVE ZONE FAILURE" in db.sql
    assert prov.topology.survival_goal == "zone"

def test_region_survival_needs_three_regions(tmp_path):
    from controlplane.topology import without_region
    two = without_region(CLASSIC, "us-east-2")
    e, *_ = make(tmp_path, topo=two)
    with pytest.raises(ElasticError, match="3 regions"):
        e.set_survival("region")


def test_set_primary_moves_the_primary_on_both_databases(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path)
    e.set_primary("us-east-2")
    assert ops.snapshot()["status"] == "done", ops.snapshot()
    assert 'ALTER DATABASE chaos SET PRIMARY REGION "us-east-2"' in db.sql
    assert prov.topology.primary.name == "us-east-2"


def test_new_nodes_join_through_live_nodes_when_the_first_ones_are_down(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path)
    for n in CLASSIC.nodes[:2]:  # the primary region is down
        docker.containers[n.name] = ContainerInfo(n.name, n.name, False, "exited", {}, {})
    e.add_node("us-west-2")
    joined = docker.ran[0][1]
    assert not any(j.startswith("us-east-1") for j in joined) and joined


def test_a_failed_add_removes_what_it_started_and_a_retry_works(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path)
    import controlplane.elastic as el
    el.JOIN_ATTEMPTS, saved = 1, el.JOIN_ATTEMPTS
    try:
        db.join_fails = True
        e.add_node("us-east-2")
        assert ops.snapshot()["status"] == "failed" and "us-east-2b-n2" in docker.removed
        db.join_fails = False
        e.add_node("us-east-2")
        assert ops.snapshot()["status"] == "done"
    finally:
        el.JOIN_ATTEMPTS = saved


def test_remove_region_is_retry_safe_and_commits_the_goal_change_early(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path, topo=AWS)
    db.fail_on = 'DROP REGION IF EXISTS "us-west-2"'
    e.remove_region("us-west-2")
    assert ops.snapshot()["status"] == "failed"
    assert prov.topology.survival_goal == "zone" and prov.topology.secondary is None
    assert all("DROP SECONDARY REGION IF EXISTS" in s for s in db.sql if "SECONDARY" in s)
    db.fail_on = None
    e.remove_region("us-west-2")
    assert ops.snapshot()["status"] == "done", ops.snapshot()


def test_a_topology_change_between_validation_and_start_is_refused(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path)
    original_start = ops.start
    def start(kind):
        prov.topology = AWS  # another change committed meanwhile
        return original_start(kind)
    ops.start = start
    e.add_node("us-east-2")
    assert ops.snapshot()["status"] == "failed" and "changed" in ops.snapshot()["error"]


def test_schema_changes_retry_transient_errors(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path, topo=AWS9)
    db.fail_once = "SURVIVE ZONE FAILURE"
    e.set_survival("zone")
    assert ops.snapshot()["status"] == "done", ops.snapshot()


def test_zone_survival_needs_three_nodes_in_the_primary_region(tmp_path):
    from controlplane.topology import load_preset as lp
    six = lp(ROOT / "presets", "aws", CAT, **KW)  # 3 regions x 2 nodes
    e, *_ = make(tmp_path, topo=six)
    with pytest.raises(ElasticError, match="3 nodes in us-east-1"):
        e.set_survival("zone")


def test_decommission_waits_out_a_temporarily_suspect_store(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path)
    refusals = ['ranges blocking decommission detected\nn7 has 6 replicas blocked with error: "1 matching stores are '
                'currently throttled: [s2: suspect]"\nERROR: Cannot decommission nodes.'] * 3
    real_exec = docker.exec

    def flaky_exec(name, cmd, timeout=60.0, check=True):
        if "decommission" in cmd and "--wait=none" in cmd and refusals:
            return subprocess.CompletedProcess(cmd, 1, "", refusals.pop(0))
        return real_exec(name, cmd, timeout, check)
    docker.exec = flaky_exec
    e.decommission("us-west-2b-n2")
    op = ops.snapshot()
    assert op["status"] == "done", op
    assert any("settle" in s for s in op["steps"])


def test_decommission_still_fails_fast_on_a_real_refusal(tmp_path):
    e, docker, db, prov, lb, chaos, ops = make(tmp_path, decommission_rc=1)  # "not enough nodes"
    e.decommission("us-west-2b-n2")
    op = ops.snapshot()
    assert op["status"] == "failed" and "not enough nodes" in op["error"]
    assert len([c for _, c in docker.execs if "decommission" in c]) == 1  # no pointless retries
