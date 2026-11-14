import re
from pathlib import Path

import pytest

from controlplane.catalog import load_catalog
from controlplane.topology import PresetError, list_presets, load_preset, parse_preset

ROOT = Path(__file__).resolve().parents[2]
CATALOG = load_catalog(ROOT / "catalog" / "regions.yaml")
KW = dict(default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)


def parse(data, name="t"):
    return parse_preset(name, data, CATALOG, **KW)


def aws3():
    return {"display_name": "Acme Payments", "cluster_id": "acme-payments", "regions": [
        {"name": "us-east-1", "provider": "aws", "nodes": 3, "primary": True},
        {"name": "us-west-2", "provider": "aws", "nodes": 3, "secondary": True},
        {"name": "eu-west-1", "provider": "aws", "nodes": 3},
    ]}


def test_nodes_get_cloud_style_names_zones_and_region_subnets():
    t = parse(aws3())
    assert [n.name for n in t.nodes[:3]] == ["us-east-1a-n1", "us-east-1b-n2", "us-east-1c-n3"]
    assert [n.ip for n in t.nodes[:3]] == ["10.77.1.11", "10.77.1.12", "10.77.1.13"]
    assert (t.nodes[3].name, t.nodes[3].ip) == ("us-west-2a-n1", "10.77.2.11")
    assert t.nodes[-1].ip == "10.77.3.13"
    assert t.image == "docker.io/cockroachdb/cockroach:v26.3.2"
    assert t.primary.name == "us-east-1" and t.secondary.name == "us-west-2"
    assert t.regions[0].label == "N. Virginia" and t.regions[0].location == (38.9, -77.4)


def test_zones_round_robin_when_a_region_has_more_nodes_than_zones():
    t = parse({"regions": [
        {"name": "us-east-1", "provider": "aws", "nodes": 4},
        {"name": "us-west-2", "provider": "aws", "nodes": 1},
        {"name": "eu-west-1", "provider": "aws", "nodes": 1},
    ]})
    assert [n.zone for n in t.nodes if n.region == "us-east-1"] == ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1a"]


def test_catalog_supplies_real_zone_names_and_rules_cover_unknown_regions():
    t = parse({"regions": [
        {"name": "eastus", "provider": "azure", "nodes": 2},
        {"name": "us-east1", "provider": "gcp", "nodes": 1},
        {"name": "dc-frankfurt", "provider": "onprem", "nodes": 1, "location": [50.1, 8.7]},
    ]})
    assert [n.name for n in t.nodes] == ["eastus-1-n1", "eastus-2-n2", "us-east1-b-n1", "dc-frankfurt-a-n1"]
    assert t.regions[2].label == "dc-frankfurt" and t.regions[2].location == (50.1, 8.7)


def test_colliding_hostnames_get_a_provider_prefix():
    t = parse({"regions": [
        {"name": "edge-1", "provider": "custom", "nodes": 1, "zones": ["edge-z"]},
        {"name": "edge-2", "provider": "onprem", "nodes": 1, "zones": ["edge-z"]},
        {"name": "edge-3", "provider": "custom", "nodes": 1},
    ]})
    assert [n.name for n in t.nodes] == ["custom-edge-z-n1", "onprem-edge-z-n1", "edge-3-a-n1"]


@pytest.mark.parametrize("counts, expected", [
    ([3, 3, 3], "region"),
    ([2, 2, 1], "region"),
    ([1, 1, 1], "none"),
    ([3, 1], "zone"),
    ([2, 2], "none"),
])
def test_auto_survival_goal(counts, expected):
    data = {"regions": [{"name": f"r{i}", "provider": "custom", "nodes": n} for i, n in enumerate(counts)]}
    assert parse(data).survival_goal == expected


def test_explicit_goal_that_cannot_be_satisfied_produces_a_warning():
    t = parse({"survival_goal": "zone", "regions": [{"name": f"r{i}", "nodes": 1} for i in range(3)]})
    assert t.survival_goal == "zone"
    assert any("ZONE survival needs 3+ nodes in every region" in w for w in t.warnings)
    # a third node in the primary alone isn't enough: each region's payments keep their three copies at home
    t = parse({"survival_goal": "zone", "regions": [{"name": "r0", "nodes": 3}, {"name": "r1", "nodes": 2}, {"name": "r2", "nodes": 3}]})
    assert any("ZONE survival needs 3+ nodes in every region" in w and "r1" in w for w in t.warnings)


def test_first_region_becomes_primary_when_none_is_marked():
    t = parse({"regions": [{"name": "a1", "nodes": 1}, {"name": "b1", "nodes": 1}]})
    assert t.primary.name == "a1" and t.regions[0].primary


@pytest.mark.parametrize("data, message", [
    ({}, "'regions' must be a non-empty list"),
    ({"regions": [{"name": "US-East-1", "nodes": 1}]}, "lowercase letters, digits and hyphens"),
    ({"regions": [{"name": "a1", "nodes": "3"}]}, "'nodes' between 1 and 12"),
    ({"regions": [{"name": "a1", "nodes": 13}]}, "'nodes' between 1 and 12"),
    ({"regions": [{"name": "a1", "nodes": 1}, {"name": "a1", "nodes": 1}]}, "listed twice"),
    ({"regions": [{"name": "a1", "nodes": 1, "primary": True}, {"name": "b1", "nodes": 1, "primary": True}]}, "only one region can be primary"),
    ({"regions": [{"name": "a1", "nodes": 1, "location": [91, 0]}]}, "location"),
    ({"survival_goal": "planet", "regions": [{"name": "a1", "nodes": 1}]}, "survival_goal"),
    ({"regions": [{"name": f"r{i}", "nodes": 4} for i in range(4)]}, "exceeds MAX_NODES=15"),
    ({"regions": [{"name": "a1", "nodes": 1, "zones": ["Zone A"]}]}, "zone name"),
    ({"regions": [{"name": "a1", "nodes": 1, "zones": 3}]}, "'zones' must be a list"),
    ({"node_memory": "512MiB", "regions": [{"name": "a1", "nodes": 1}]}, "'node_memory' must be a mapping"),
    ({"survival_goal": "region", "regions": [{"name": "a1", "nodes": 3}, {"name": "b1", "nodes": 3}]}, "REGION survival needs at least 3 regions"),
])
def test_invalid_presets_explain_what_is_wrong(data, message):
    with pytest.raises(PresetError, match=re.escape(message)):
        parse(data)


def test_subnet_must_be_an_ipv4_slash_16():
    with pytest.raises(PresetError, match="IPv4 /16"):
        parse_preset("t", aws3(), CATALOG, default_version="v26.3.2", subnet="10.77.0.0/24", max_nodes=15)


def test_every_shipped_preset_parses_without_warnings():
    names = list_presets(ROOT / "presets")
    assert {"aws", "gcp", "azure", "ibm", "multi-cloud", "classic", "small"} <= set(names)
    for name in names:
        t = load_preset(ROOT / "presets", name, CATALOG, **KW)
        assert t.nodes and not t.warnings, name


def test_shipped_presets_resolve_the_expected_goals():
    goals = {n: load_preset(ROOT / "presets", n, CATALOG, **KW).survival_goal for n in ("aws", "classic", "small")}
    assert goals == {"aws": "region", "classic": "region", "small": "none"}


def test_unknown_preset_and_invalid_yaml(tmp_path):
    with pytest.raises(PresetError, match="unknown preset"):
        load_preset(tmp_path, "nope", CATALOG, **KW)
    (tmp_path / "broken.yaml").write_text("regions: [", encoding="utf-8")
    with pytest.raises(PresetError, match="invalid YAML"):
        load_preset(tmp_path, "broken", CATALOG, **KW)


def test_custom_presets_live_in_the_custom_folder(tmp_path):
    (tmp_path / "custom").mkdir()
    (tmp_path / "custom" / "mine.yaml").write_text("regions:\n  - {name: us-east-1, provider: aws, nodes: 1}\n", encoding="utf-8")
    (tmp_path / "custom" / "README.md").write_text("# Your own presets\n", encoding="utf-8")  # shipped in the folder
    assert load_preset(tmp_path, "mine", CATALOG, **KW).nodes[0].name == "us-east-1a-n1"
    assert list_presets(tmp_path) == ["mine"]


def test_preset_names_are_case_insensitive():
    from controlplane.topology import load_preset as lp
    t = lp(ROOT / "presets", " AWS ", CATALOG, default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
    assert t.preset == "aws"


def test_node_memory_accepts_a_go_limit_and_defaults_to_1gib():
    from controlplane.topology import parse_preset
    base = {"regions": [{"name": "us-east-1", "provider": "aws", "nodes": 1}]}
    assert parse_preset("x", base, CATALOG, default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15).go_memory == "1GiB"
    custom = {**base, "node_memory": {"go": "2GiB"}}
    assert parse_preset("x", custom, CATALOG, default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15).go_memory == "2GiB"


def test_single_region_model_keeps_the_primary_region_with_one_node_per_zone():
    from controlplane.topology import load_preset as lp
    kw = dict(default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
    topo = lp(ROOT / "presets", "azure", CATALOG, model="single-region", **kw)
    assert [r.name for r in topo.regions] == ["eastus"] and topo.regions[0].primary and not topo.regions[0].secondary
    assert len(topo.nodes) == 3 and len({n.zone for n in topo.nodes}) == 3
    assert topo.survival_goal == "zone" and topo.model == "single-region"
    assert lp(ROOT / "presets", "azure", CATALOG, **kw).model == "multi-region"


def test_deployment_model_is_validated():
    import pytest
    from controlplane.topology import PresetError, load_preset as lp
    with pytest.raises(PresetError, match="deployment model"):
        lp(ROOT / "presets", "aws", CATALOG, default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15, model="two-dc")


def stretch():
    return {"display_name": "Acme Payments", "cluster_id": "acme-payments", "regions": [
        {"name": "us-east-1", "provider": "aws", "nodes": 2, "primary": True},
        {"name": "eastus", "provider": "azure", "nodes": 2, "secondary": True},
        {"name": "us-east4", "provider": "gcp", "nodes": 1, "tie_breaker": True},
    ]}


def test_a_tie_breaker_region_is_marked_and_the_others_are_not():
    t = parse(stretch())
    assert [r.name for r in t.tie_breakers] == ["us-east4"]
    assert [r.name for r in t.regions if not r.tie_breaker] == ["us-east-1", "eastus"]
    assert t.survival_goal == "region" and not t.warnings


def test_the_stretch_preset_ships_2_aws_2_azure_and_a_gcp_tie_breaker_in_one_metro():
    t = load_preset(ROOT / "presets", "stretch", CATALOG, **KW)
    assert [(r.name, r.provider, r.nodes, r.primary, r.secondary, r.tie_breaker) for r in t.regions] == [
        ("us-east-1", "aws", 2, True, False, False), ("eastus", "azure", 2, False, True, False),
        ("us-east4", "gcp", 1, False, False, True)]
    assert t.survival_goal == "region"


@pytest.mark.parametrize("index, extra, message", [
    (0, {"tie_breaker": True}, "the tie-breaker can't be the primary region"),
    (1, {"tie_breaker": True}, "the tie-breaker can't be the secondary region"),
    (1, {"tie_breaker": True, "secondary": False}, "needs two regions besides the tie-breaker"),
])
def test_tie_breaker_roles_that_cannot_work_are_refused(index, extra, message):
    data = stretch()
    data["regions"][index] = {**data["regions"][index], **extra}
    with pytest.raises(PresetError, match=re.escape(message)):
        parse(data)


def test_a_tie_breaker_only_matters_under_region_survival():
    t = parse({**stretch(), "survival_goal": "zone"})
    assert any("tie-breaker" in w for w in t.warnings)


def test_the_single_region_model_leaves_the_tie_breaker_out():
    t = parse_preset("t", stretch(), CATALOG, model="single-region", **KW)
    assert t.tie_breakers == () and [r.name for r in t.regions] == ["us-east-1"]


def test_saved_topologies_keep_the_tie_breaker_and_older_ones_still_load():
    from controlplane.state import topology_from_dict, topology_to_dict
    t = parse(stretch())
    assert topology_from_dict(topology_to_dict(t)) == t
    old = topology_to_dict(t)
    for r in old["regions"]:
        r.pop("tie_breaker")
    assert topology_from_dict(old).tie_breakers == ()


def test_the_residency_preset_is_three_regions_of_three_nodes():
    # three regions so the cluster itself survives losing one; three nodes each so a domiciled row keeps all three
    # copies at home, one per availability zone
    t = load_preset(ROOT / "presets", "residency", CATALOG, **KW)
    assert [(r.name, r.nodes, len({n.zone for n in t.nodes if n.region == r.name})) for r in t.regions] == [
        ("us-east-1", 3, 3), ("eu-west-1", 3, 3), ("ap-southeast-1", 3, 3)]
    assert t.survival_goal == "region"
