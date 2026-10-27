from pathlib import Path

import pytest

from controlplane.catalog import load_catalog
from controlplane.topology import PresetError, load_preset, with_added_node, with_added_region, without_nodes, without_region

ROOT = Path(__file__).resolve().parents[2]
CAT = load_catalog(ROOT / "catalog" / "regions.yaml")
KW = dict(default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
CLASSIC = load_preset(ROOT / "presets", "classic", CAT, **KW)


def test_added_node_goes_to_the_emptiest_zone_with_the_next_ordinal_and_ip():
    topo, node = with_added_node(CLASSIC, "us-east-2", subnet="10.77.0.0/16", max_nodes=15)
    assert node.region == "us-east-2" and node.ordinal == 2 and node.ip == "10.77.3.12"
    assert node.zone == "us-east-2b" and node.name == "us-east-2b-n2"
    assert next(r for r in topo.regions if r.name == "us-east-2").nodes == 2
    assert topo.nodes[-1] == node and len(topo.nodes) == 6


def test_added_node_respects_limits_and_unknown_regions():
    with pytest.raises(PresetError, match="MAX_NODES"):
        with_added_node(CLASSIC, "us-east-2", subnet="10.77.0.0/16", max_nodes=5)
    with pytest.raises(PresetError, match="no region"):
        with_added_node(CLASSIC, "mars-1", subnet="10.77.0.0/16", max_nodes=15)


def test_removed_nodes_update_region_counts_and_free_their_ordinal():
    topo = without_nodes(CLASSIC, {"us-east-1b-n2"})
    assert [r.nodes for r in topo.regions] == [1, 2, 1]
    topo, node = with_added_node(topo, "us-east-1", subnet="10.77.0.0/16", max_nodes=15)
    assert node.ordinal == 2 and node.zone == "us-east-1b"


def test_added_region_uses_the_catalog_and_the_next_subnet():
    topo = with_added_region(CLASSIC, CAT, name="eu-west-1", provider="aws", nodes=3, subnet="10.77.0.0/16", max_nodes=15)
    region = topo.regions[-1]
    assert region.index == 4 and region.label and region.location and not region.primary
    assert [n.ip for n in topo.nodes if n.region == "eu-west-1"] == ["10.77.4.11", "10.77.4.12", "10.77.4.13"]


def test_added_region_validation():
    with pytest.raises(PresetError, match="already"):
        with_added_region(CLASSIC, CAT, name="us-east-1", provider="aws", nodes=1, subnet="10.77.0.0/16", max_nodes=15)
    with pytest.raises(PresetError, match="lowercase"):
        with_added_region(CLASSIC, CAT, name="Bad Name", provider="aws", nodes=1, subnet="10.77.0.0/16", max_nodes=15)
    with pytest.raises(PresetError, match="MAX_NODES"):
        with_added_region(CLASSIC, CAT, name="eu-west-1", provider="aws", nodes=3, subnet="10.77.0.0/16", max_nodes=6)


def test_removing_a_region_refuses_the_primary_and_clears_the_secondary_flag():
    with pytest.raises(PresetError, match="primary"):
        without_region(CLASSIC, "us-east-1")
    topo = without_region(CLASSIC, "us-west-2")
    assert [r.name for r in topo.regions] == ["us-east-1", "us-east-2"]
    assert all(n.region != "us-west-2" for n in topo.nodes) and topo.secondary is None


def test_adding_or_removing_regions_updates_the_deployment_model():
    one = load_preset(ROOT / "presets", "aws", CAT, model="single-region", **KW)
    two = with_added_region(one, CAT, name="us-west-2", provider="aws", nodes=1, subnet="10.77.0.0/16", max_nodes=15)
    assert one.model == "single-region" and two.model == "multi-region"
    assert without_region(two, "us-west-2").model == "single-region"
