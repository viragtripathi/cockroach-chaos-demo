from pathlib import Path

from controlplane.catalog import load_catalog
from controlplane.docker_ops import ContainerInfo
from controlplane.snapshot import assemble_snapshot
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
TOPO = load_preset(ROOT / "presets", "classic", load_catalog(ROOT / "catalog" / "regions.yaml"),
                   default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)


def running(*names, stopped=()):
    return [ContainerInfo(n, n, n not in stopped, "running" if n not in stopped else "exited", {}, {}) for n in names]


def rows(live=None):
    live = live or {n.name: True for n in TOPO.nodes}
    return [{"node_id": i + 1, "address": f"{name}:26257", "is_live": ok, "ranges": 50, "leases": 10}
            for i, (name, ok) in enumerate(live.items())]


def test_healthy_cluster():
    snap = assemble_snapshot(TOPO, running(*[n.name for n in TOPO.nodes]), rows(), {"unavailable": 0, "underreplicated": 0}, None, 1.0)
    assert snap["cluster"]["status"] == "healthy"
    assert (snap["cluster"]["live_nodes"], snap["cluster"]["total_nodes"]) == (5, 5)
    assert snap["cluster"]["providers"] == ["aws"]
    assert [r["state"] for r in snap["regions"]] == ["healthy"] * 3
    node = snap["nodes"][0]
    assert node == {"name": "us-east-1a-n1", "region": "us-east-1", "zone": "us-east-1a", "provider": "aws",
                    "ip": "10.77.1.11", "runtime": "running", "liveness": "live", "node_id": 1, "replicas": 50, "leases": 10,
                    "partitioned": False, "brownout": None, "isolated": False}
    assert snap["regions"][0]["label"] == "N. Virginia" and snap["regions"][0]["primary"] is True
    assert snap["telemetry"] == {"sql": "ok"}


def test_dead_region_degrades_the_cluster():
    live = {n.name: n.region != "us-east-1" for n in TOPO.nodes}
    names = [n.name for n in TOPO.nodes]
    snap = assemble_snapshot(TOPO, running(*names, stopped=("us-east-1a-n1", "us-east-1b-n2")), rows(live), {"unavailable": 0, "underreplicated": 12}, None, 1.0)
    assert snap["cluster"]["status"] == "degraded"
    assert snap["regions"][0]["state"] == "down" and snap["regions"][1]["state"] == "healthy"
    assert snap["nodes"][0]["runtime"] == "stopped" and snap["nodes"][0]["liveness"] == "dead"
    assert snap["ranges"] == {"unavailable": 0, "underreplicated": 12}


def test_unavailable_ranges_are_critical():
    snap = assemble_snapshot(TOPO, running(*[n.name for n in TOPO.nodes]), rows(), {"unavailable": 3, "underreplicated": 0}, None, 1.0)
    assert snap["cluster"]["status"] == "critical"


def test_missing_sql_telemetry_is_unreachable_not_zero():
    snap = assemble_snapshot(TOPO, running(*[n.name for n in TOPO.nodes]), None, None, None, 1.0)
    assert snap["cluster"]["status"] == "unreachable"
    assert {n["liveness"] for n in snap["nodes"]} == {"unknown"}
    assert snap["ranges"] == {"unavailable": None, "underreplicated": None}
    assert snap["telemetry"] == {"sql": "delayed"}


def test_running_operation_shows_provisioning_and_nodes_not_yet_gossiped_are_joining():
    op = {"id": "1", "kind": "provision", "status": "running", "steps": ["Allocating capacity in us-east-1"], "error": None}
    snap = assemble_snapshot(TOPO, running("us-east-1a-n1"), [], None, op, 1.0)
    assert snap["cluster"]["status"] == "provisioning"
    assert snap["nodes"][0]["liveness"] == "joining" and snap["nodes"][1]["runtime"] == "missing"
    assert snap["operation"] == op


def test_no_topology_is_empty():
    snap = assemble_snapshot(None, [], None, None, None, 1.0)
    assert snap["cluster"] is None and snap["regions"] == [] and snap["nodes"] == []


def test_chaos_flags_and_partitions_show_up_on_nodes():
    names = [n.name for n in TOPO.nodes]
    containers = [ContainerInfo(n, n, True, "running", {}, {} if n == "us-east-1a-n1" else {"crdb-chaos": "x"}) for n in names]
    flags = {"us-west-2a-n1": {"brownout": {"delay_ms": 700, "jitter_ms": 233, "loss_pct": 2.0}, "isolated": False},
             "us-east-2a-n1": {"brownout": None, "isolated": True}}
    snap = assemble_snapshot(TOPO, containers, rows(), {"unavailable": 0, "underreplicated": 0}, None, 1.0,
                             network="crdb-chaos", flags=flags, latency="realistic")
    by = {n["name"]: n for n in snap["nodes"]}
    assert by["us-east-1a-n1"]["partitioned"] is True and by["us-east-1b-n2"]["partitioned"] is False
    assert by["us-west-2a-n1"]["brownout"]["delay_ms"] == 700 and by["us-east-2a-n1"]["isolated"] is True
    assert by["us-east-1b-n2"]["brownout"] is None and by["us-east-1b-n2"]["isolated"] is False
    assert snap["cluster"]["latency"] == "realistic"


def test_elastic_operations_do_not_hide_the_real_cluster_status():
    op = {"id": "1", "kind": "add-node", "status": "running", "steps": [], "error": None}
    snap = assemble_snapshot(TOPO, running(*[n.name for n in TOPO.nodes]), rows(), {"unavailable": 0, "underreplicated": 0}, op, 1.0)
    assert snap["cluster"]["status"] == "healthy" and snap["operation"] == op


def test_a_reused_address_reports_the_newest_node():
    r = rows()
    r.insert(0, {"node_id": 99, "address": "us-east-1a-n1:26257", "is_live": True, "ranges": 1, "leases": 1})
    r.append({"node_id": 0, "address": "us-east-1a-n1:26257", "is_live": False, "ranges": 0, "leases": 0})
    snap = assemble_snapshot(TOPO, running(*[n.name for n in TOPO.nodes]), r, {"unavailable": 0, "underreplicated": 0}, None, 1.0)
    assert snap["nodes"][0]["node_id"] == 99


def test_snapshot_reports_the_deployment_model():
    snap = assemble_snapshot(TOPO, running(*[n.name for n in TOPO.nodes]), rows(), {"unavailable": 0, "underreplicated": 0}, None, 1.0)
    assert snap["cluster"]["model"] == "multi-region"
