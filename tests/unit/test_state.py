from pathlib import Path

from controlplane.catalog import load_catalog
from controlplane.state import load_state, save_state, topology_from_dict, topology_to_dict
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
TOPO = load_preset(ROOT / "presets", "classic", load_catalog(ROOT / "catalog" / "regions.yaml"),
                   default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)


def test_topology_round_trips_through_json_shapes():
    assert topology_from_dict(topology_to_dict(TOPO)) == TOPO


def test_save_then_load(tmp_path: Path):
    save_state(tmp_path, {"topology": topology_to_dict(TOPO), "provisioned": True})
    state = load_state(tmp_path)
    assert state["provisioned"] is True
    assert topology_from_dict(state["topology"]).nodes == TOPO.nodes
    assert not list(tmp_path.glob(".state-*")), "temporary files are cleaned up"


def test_missing_or_corrupt_state_is_empty(tmp_path: Path):
    assert load_state(tmp_path) == {}
    (tmp_path / "state.json").write_text("{not json", encoding="utf-8")
    assert load_state(tmp_path) == {}


def test_state_saved_before_go_memory_existed_still_loads():
    from controlplane.state import topology_from_dict, topology_to_dict
    d = topology_to_dict(TOPO)
    d.pop("go_memory", None)
    assert topology_from_dict(d).go_memory == "1GiB"
