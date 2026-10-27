import random

from controlplane.homing import gateway_candidates, home_of, region_order, workers_by_region

REGIONS = [{"name": "us-east-1", "location": [38.9, -77.4], "primary": True},
           {"name": "us-west-2", "location": [45.8, -119.7], "primary": False},
           {"name": "eu-west-1", "location": [53.3, -6.3], "primary": False}]


def node(name, region, ip, **over):
    return {"name": name, "region": region, "ip": ip, "liveness": "live", "runtime": "running",
            "partitioned": False, "isolated": False, **over}


NODES = [node("us-east-1a-n1", "us-east-1", "10.77.1.11"), node("us-east-1b-n2", "us-east-1", "10.77.1.12"),
         node("us-west-2a-n1", "us-west-2", "10.77.2.11"), node("eu-west-1a-n1", "eu-west-1", "10.77.3.11")]


def test_workers_and_homes_split_round_robin():
    regions = [r["name"] for r in REGIONS]
    assert [home_of(i, regions) for i in range(5)] == ["us-east-1", "us-west-2", "eu-west-1", "us-east-1", "us-west-2"]
    assert workers_by_region(48, regions) == {"us-east-1": 16, "us-west-2": 16, "eu-west-1": 16}
    assert workers_by_region(4, regions) == {"us-east-1": 2, "us-west-2": 1, "eu-west-1": 1}


def test_regions_are_tried_home_first_then_by_distance():
    assert region_order("us-east-1", REGIONS) == ["us-east-1", "us-west-2", "eu-west-1"]
    assert region_order("eu-west-1", REGIONS) == ["eu-west-1", "us-east-1", "us-west-2"]
    blind = [dict(r, location=None) for r in REGIONS]
    assert region_order("us-west-2", blind) == ["us-west-2", "us-east-1", "eu-west-1"]


def test_gateway_candidates_skip_unusable_nodes_and_fall_back_by_distance():
    snap = {"regions": REGIONS, "nodes": NODES}
    cands = gateway_candidates("us-east-1", snap, random.Random(1))
    assert [c[1] for c in cands] == ["us-east-1", "us-east-1", "us-west-2", "eu-west-1"]
    assert {c[0] for c in cands[:2]} == {"10.77.1.11", "10.77.1.12"}
    down = [dict(n, liveness="dead") if n["region"] == "us-east-1" else n for n in NODES]
    assert gateway_candidates("us-east-1", {"regions": REGIONS, "nodes": down}, random.Random(1))[0] == ("10.77.2.11", "us-west-2")
    flagged = [dict(NODES[0], isolated=True), dict(NODES[1], partitioned=True), dict(NODES[2], runtime="stopped")]
    assert gateway_candidates("us-east-1", {"regions": REGIONS, "nodes": flagged}, random.Random(1)) == []
    assert gateway_candidates("us-east-1", {}, random.Random(1)) == []
