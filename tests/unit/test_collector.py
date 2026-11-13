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


# -- a lapsed liveness record is not, by itself, a dead node --

N2 = NodeSpec("us-east-1b-n2", "us-east-1", "us-east-1b", "aws", 2, "10.77.1.12")
PAIR = Topology("t", "T", "t", "v26.3.2", "none", "realistic", "256MiB", "256MiB", (REGION,), (NODE, N2))
PAIR_NODES = {"nodes": [
    {"desc": {"nodeId": 1, "address": {"addressField": "us-east-1a-n1:26257"}},
     "storeStatuses": [{"metrics": {"replicas": 40, "replicas.leaseholders": 12, "ranges.unavailable": 0, "ranges.underreplicated": 3}}]},
    {"desc": {"nodeId": 2, "address": {"addressField": "us-east-1b-n2:26257"}},
     "storeStatuses": [{"metrics": {"replicas": 41, "replicas.leaseholders": 9, "ranges.unavailable": 0, "ranges.underreplicated": 4}}]},
]}


def records(expires: dict) -> dict:
    return {"livenesses": [{"nodeId": i, "expiration": {"wallTime": str(int(t * 1e9))}, "membership": 0} for i, t in expires.items()]}


def test_running_nodes_stay_live_through_a_short_lapse_of_their_liveness_records():
    """When the node leading CockroachDB's liveness range dies, no node can renew its record until the range has a
    new leader: every record lapses for a few seconds although the nodes are up and serving."""
    both = {"us-east-1a-n1", "us-east-1b-n2"}
    lapse = records({1: NOW + 4, 2: NOW + 4})
    live = lambda now, liveness=lapse: [r["is_live"] for r in parse_status(PAIR_NODES, liveness, now, vouched=both)[0]]  # noqa: E731
    assert live(NOW) == [True, True]
    assert live(NOW + 5) == [True, True]          # lapsed 1 s ago
    assert live(NOW + 11.9) == [True, True]       # 7.9 s ago
    assert live(NOW + 12) == [False, False]       # 8 s: a record this stale means the node is down
    assert live(NOW + 13, records({1: NOW + 17, 2: NOW + 17})) == [True, True]   # renewed


def test_a_node_the_panel_cannot_vouch_for_gets_no_grace():
    rows, _ = parse_status(PAIR_NODES, records({1: NOW - 1, 2: NOW - 1}), NOW, vouched={"us-east-1a-n1"})
    assert [r["is_live"] for r in rows] == [True, False]
    rows, _ = parse_status(PAIR_NODES, records({1: NOW - 1, 2: NOW - 1}), NOW)   # nobody vouched for: the record alone decides
    assert [r["is_live"] for r in rows] == [False, False]


def test_a_node_riding_out_a_lapse_still_reports_its_ranges():
    _, ranges = parse_status(PAIR_NODES, records({1: NOW - 1, 2: NOW - 1}), NOW, vouched={"us-east-1a-n1", "us-east-1b-n2"})
    assert ranges == {"unavailable": 0, "underreplicated": 7}
    _, ranges = parse_status(PAIR_NODES, records({1: NOW - 1, 2: NOW - 1}), NOW)
    assert ranges == {"unavailable": 0, "underreplicated": 0}


def test_crashing_the_liveness_leader_shows_one_node_down_not_the_cluster():
    """The stage showed 2 of 9 nodes live for a few seconds after one node was crashed: every other record had lapsed.
    Now: the crashed node (its container is stopped) is down as soon as its record lapses; the running ones are not."""
    class PairDocker:
        def list_nodes(self):
            return [ContainerInfo(NODE.name, "1", True, "running", {}, {"crdb-chaos": NODE.ip}),
                    ContainerInfo(N2.name, "2", False, "exited", {}, {})]
    now = [NOW]
    fetch = lambda host, path: PAIR_NODES if path == "/_status/nodes" else records({1: NOW + 4, 2: NOW + 4})  # noqa: E731
    c = Collector(PairDocker(), SimpleNamespace(topology=PAIR), Operations(), status_host="haproxy", fetch_json=fetch,
                  clock=lambda: now[0], network="crdb-chaos")
    assert [n["liveness"] for n in c.refresh()["nodes"]] == ["live", "live"]   # the cluster hasn't noticed the crash yet
    now[0] = NOW + 5
    snap = c.refresh()
    assert [n["liveness"] for n in snap["nodes"]] == ["live", "dead"]
    assert snap["cluster"]["live_nodes"] == 1 and snap["cluster"]["status"] == "degraded"


def test_a_partitioned_or_isolated_node_is_down_as_soon_as_its_record_lapses():
    class Detached:
        def list_nodes(self):
            return [ContainerInfo(NODE.name, "1", True, "running", {}, {}),                     # off the cluster network
                    ContainerInfo(N2.name, "2", True, "running", {}, {"crdb-chaos": N2.ip})]
    fetch = lambda host, path: PAIR_NODES if path == "/_status/nodes" else records({1: NOW - 1, 2: NOW - 1})  # noqa: E731
    chaos = SimpleNamespace(flags=lambda: {N2.name: {"brownout": None, "isolated": True}}, latency_setting=lambda: "realistic")
    snap = Collector(Detached(), SimpleNamespace(topology=PAIR), Operations(), status_host="haproxy", fetch_json=fetch,
                     clock=lambda: NOW, network="crdb-chaos", chaos=chaos).refresh()
    assert [n["liveness"] for n in snap["nodes"]] == ["dead", "dead"]
