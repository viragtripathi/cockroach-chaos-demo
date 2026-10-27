from pathlib import Path

import pytest

from controlplane.catalog import load_catalog
from controlplane.netem import Brownout, latency_scale, rtt_ms, tc_script
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
AWS = load_preset(ROOT / "presets", "aws", load_catalog(ROOT / "catalog" / "regions.yaml"),
                  default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
EAST, WEST, EU = AWS.regions
NODE = AWS.nodes[0]  # us-east-1a-n1


def test_rtt_follows_distance_and_scale():
    east_west = rtt_ms(EAST, WEST, 1.0)
    east_eu = rtt_ms(EAST, EU, 1.0)
    assert 45 < east_west < 75 and east_eu > east_west
    assert rtt_ms(EAST, WEST, 2.0) == pytest.approx(2 * east_west)


@pytest.mark.parametrize("setting, expected", [("off", None), ("realistic", 1.0), ("1.5", 1.5), ("bogus", 1.0)])
def test_latency_setting_maps_to_a_scale(setting, expected):
    assert latency_scale(setting) == expected


def test_clean_node_only_removes_shaping():
    script = tc_script(NODE, AWS, prefix="10.77", scale=None, brownout=None, isolated=False)
    assert script.strip() == "tc qdisc del dev eth0 root 2>/dev/null; true"


def test_realistic_latency_shapes_traffic_per_remote_region_only():
    script = tc_script(NODE, AWS, prefix="10.77", scale=1.0, brownout=None, isolated=False)
    lines = script.splitlines()
    assert "tc qdisc add dev eth0 root handle 1: prio bands 3 priomap " + " ".join(["0"] * 16) in lines
    assert "tc qdisc add dev eth0 parent 1:1 handle 10: netem limit 10000 delay 0ms" in lines
    half_west = round(rtt_ms(EAST, WEST, 1.0) / 2)
    assert f"tc qdisc add dev eth0 parent 1:2 handle 20: netem limit 10000 delay {half_west}ms" in lines
    assert "tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.77.2.0/24 flowid 1:2" in lines
    assert "tc filter add dev eth0 parent 1: protocol ip prio 1 u32 match ip dst 10.77.3.0/24 flowid 1:3" in lines
    assert "10.77.1.0/24" not in script  # own region is never delayed


def test_brownout_slows_everything_including_clients():
    script = tc_script(NODE, AWS, prefix="10.77", scale=None, brownout=Brownout(700, 200, 2.0), isolated=False)
    assert "parent 1:1 handle 10: netem limit 10000 delay 700ms 200ms loss 2.0%" in script
    assert "parent 1:2 handle 20: netem limit 10000 delay 700ms 200ms loss 2.0%" in script


def test_isolation_drops_cross_region_traffic_but_keeps_clients():
    script = tc_script(NODE, AWS, prefix="10.77", scale=1.0, brownout=None, isolated=True)
    assert "parent 1:1 handle 10: netem limit 10000 delay 0ms" in script
    assert "parent 1:2 handle 20: netem limit 10000" in script and "loss 100%" in script
    assert script.count("loss 100%") == 2


def test_single_region_needs_no_shaping_and_brownout_still_works():
    from pathlib import Path
    from controlplane.catalog import load_catalog
    from controlplane.netem import Brownout, tc_script
    from controlplane.topology import load_preset
    root = Path(__file__).resolve().parents[2]
    topo = load_preset(root / "presets", "aws", load_catalog(root / "catalog" / "regions.yaml"), default_version="v26.3.2",
                       subnet="10.77.0.0/16", max_nodes=15, model="single-region")
    node = topo.nodes[0]
    plain = tc_script(node, topo, prefix="10.77", scale=1.0, brownout=None, isolated=False)
    assert "qdisc add" not in plain  # nothing to delay: no other regions
    slow = tc_script(node, topo, prefix="10.77", scale=1.0, brownout=Brownout(700, 200, 2.0), isolated=False)
    assert "bands 2" in slow and "delay 700ms" in slow  # prio needs at least 2 bands
