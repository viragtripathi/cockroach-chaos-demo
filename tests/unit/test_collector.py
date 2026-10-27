from types import SimpleNamespace

from controlplane.collector import Collector, parse_status
from controlplane.docker_ops import ContainerInfo
from controlplane.operations import Operations
from controlplane.topology import NodeSpec, RegionSpec, Topology

REGION = RegionSpec("us-east-1", "aws", 1, "N. Virginia", (38.9, -77.4), ("us-east-1a",), True, False, 1)
NODE = NodeSpec("us-east-1a-n1", "us-east-1", "us-east-1a", "aws", 1, "10.77.1.11")
TOPO = Topology("t", "T", "t", "v26.3.2", "none", "realistic", "256MiB", "256MiB", (REGION,), (NODE,))

NODES = {"nodes": [
    {"desc": {"nodeId": 1, "address": {"addressField": "us-east-1a-n1:26257"}},
     "storeStatuses": [{"metrics": {"replicas": 40, "replicas.leaseholders": 12, "ranges.unavailable": 0, "ranges.underreplicated": 2}}]},
    {"desc": {"nodeId": 2, "address": {"addressField": "old-n2:26257"}},
     "storeStatuses": [{"metrics": {"replicas": 9, "replicas.leaseholders": 1, "ranges.unavailable": 7, "ranges.underreplicated": 9}}]},
    {"desc": {"nodeId": 3, "address": {"addressField": "gone-n3:26257"}}, "storeStatuses": []},
]}
NOW = 1_800_000_000.0
LIVENESS = {
    "statuses": {"1": 2, "2": 1, "3": 5},  # connectivity-flavored view: ignored for liveness
    "livenesses": [
        {"nodeId": 1, "expiration": {"wallTime": str(int((NOW + 4) * 1e9))}, "membership": 0},  # record valid: live
        {"nodeId": 2, "expiration": {"wallTime": str(int((NOW - 30) * 1e9))}, "membership": 0},  # expired: dead
        {"nodeId": 3, "expiration": {"wallTime": str(int((NOW + 4) * 1e9))}, "membership": 2},  # decommissioned
    ],
}


class Docker:
    def list_nodes(self):
        return [ContainerInfo(NODE.name, "1", True, "running", {}, {})]


def fetcher(tried, fail=()):
    def fetch(host, path):
        tried.append((host, path))
        if host in fail:
            raise OSError("no route")
        return NODES if path == "/_status/nodes" else LIVENESS
    return fetch


def test_status_parsing_skips_decommissioned_nodes_and_counts_only_live_stores():
    rows, ranges = parse_status(NODES, LIVENESS, NOW)
    assert [r["address"] for r in rows] == ["us-east-1a-n1:26257", "old-n2:26257"]
    assert rows[0] == {"node_id": 1, "address": "us-east-1a-n1:26257", "is_live": True, "ranges": 40, "leases": 12}
    assert rows[1]["is_live"] is False
    assert ranges == {"unavailable": 0, "underreplicated": 2}  # the dead node's stale metrics don't count


def test_refresh_uses_the_http_status_api_not_sql():
    tried = []
    c = Collector(Docker(), SimpleNamespace(topology=TOPO), Operations(), status_host="haproxy", fetch_json=fetcher(tried), clock=lambda: NOW)
    snap = c.refresh()
    assert {h for h, _ in tried} == {"haproxy"}
    assert snap["nodes"][0]["liveness"] == "live" and snap["nodes"][0]["leases"] == 12
    assert snap["ranges"] == {"unavailable": 0, "underreplicated": 2}
    assert c.latest() is snap


def test_falls_back_to_a_node_when_the_load_balancer_is_unreachable():
    tried = []
    snap = Collector(Docker(), SimpleNamespace(topology=TOPO), Operations(), status_host="haproxy",
                     fetch_json=fetcher(tried, fail=("haproxy",)), clock=lambda: NOW).refresh()
    assert [h for h, _ in tried][:2] == ["haproxy", "10.77.1.11"]
    assert snap["telemetry"] == {"sql": "ok"}


def test_unreachable_cluster_yields_delayed_telemetry():
    def fetch(host, path):
        raise OSError("down")
    snap = Collector(Docker(), SimpleNamespace(topology=TOPO), Operations(), status_host="haproxy", fetch_json=fetch).refresh()
    assert snap["telemetry"] == {"sql": "delayed"} and snap["cluster"]["status"] == "unreachable"
