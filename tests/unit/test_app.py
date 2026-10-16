from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from controlplane.app import create_app, start_cluster
from controlplane.catalog import load_catalog
from controlplane.operations import Operations
from controlplane.topology import PresetError, load_preset

ROOT = Path(__file__).resolve().parents[2]
CAT = load_catalog(ROOT / "catalog" / "regions.yaml")


def load(name):
    return load_preset(ROOT / "presets", name, CAT, default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)


class Recorder:
    def __init__(self):
        self.rebuilt, self.ensured = [], []

    def rebuild(self, topo):
        self.rebuilt.append(topo.preset)

    def ensure(self, topo):
        self.ensured.append(topo.preset)


@pytest.fixture
def web(tmp_path):
    (tmp_path / "stage").mkdir()
    (tmp_path / "stage" / "index.html").write_text("<html>stage</html>", encoding="utf-8")
    (tmp_path / "shared").mkdir()
    (tmp_path / "shared" / "tokens.css").write_text(":root{}", encoding="utf-8")
    return tmp_path


def make_client(web, operations=None, provisioner=None):
    settings = SimpleNamespace(presets_dir=ROOT / "presets", web_dir=web)
    collector = SimpleNamespace(latest=lambda: {"cluster": None, "nodes": []})
    app = create_app(settings=settings, provisioner=provisioner or Recorder(), collector=collector,
                     operations=operations or Operations(), load_topology=load, run_in_background=lambda fn: fn())
    return TestClient(app)


def test_state_returns_the_latest_snapshot(web):
    assert make_client(web).get("/api/state").json() == {"cluster": None, "nodes": []}


def test_presets_lists_shipped_presets_with_their_regions(web):
    presets = {p["name"]: p for p in make_client(web).get("/api/presets").json()}
    assert presets["aws"]["survival_goal"] == "region"
    assert [r["name"] for r in presets["aws"]["regions"]] == ["us-east-1", "us-west-2", "eu-west-1"]


def test_rebuild_accepts_valid_presets(web):
    rec = Recorder()
    resp = make_client(web, provisioner=rec).post("/api/cluster/rebuild", json={"preset": "gcp"})
    assert resp.status_code == 202 and rec.rebuilt == ["gcp"]


def test_rebuild_rejects_unknown_presets_with_the_reason(web):
    resp = make_client(web).post("/api/cluster/rebuild", json={"preset": "mars"})
    assert resp.status_code == 400 and "unknown preset" in resp.json()["detail"]


def test_rebuild_is_refused_while_another_operation_runs(web):
    ops = Operations()
    ops.start("provision")
    resp = make_client(web, operations=ops).post("/api/cluster/rebuild", json={"preset": "gcp"})
    assert resp.status_code == 409


def test_stage_and_static_assets_are_served(web):
    client = make_client(web)
    assert "stage" in client.get("/").text
    assert client.get("/static/shared/tokens.css").status_code == 200


def test_events_stream_sends_snapshots(web):
    resp = make_client(web).get("/api/events?once=1")
    assert resp.headers["content-type"].startswith("text/event-stream")
    assert resp.text.startswith("data: ") and resp.text.endswith("\n\n")


def test_start_cluster_records_bad_presets_instead_of_crashing():
    ops, rec = Operations(), Recorder()
    lb = SimpleNamespace(ensure_config=lambda: None)

    def bad(name):
        raise PresetError("preset x: region name 'US' must be lowercase")

    start_cluster("x", load_topology=bad, provisioner=rec, operations=ops, lb=lb)
    op = ops.snapshot()
    assert op["status"] == "failed" and "lowercase" in op["error"] and rec.ensured == []


def test_start_cluster_writes_a_bootstrap_lb_config_then_ensures():
    calls, rec = [], Recorder()
    lb = SimpleNamespace(ensure_config=lambda: calls.append("lb"))
    start_cluster("aws", load_topology=load, provisioner=rec, operations=Operations(), lb=lb)
    assert calls == ["lb"] and rec.ensured == ["aws"]


def test_start_cluster_records_unexpected_failures_instead_of_dying_silently():
    ops = Operations()
    lb = SimpleNamespace(ensure_config=lambda: None)

    class Broken:
        def ensure(self, topo):
            raise RuntimeError("permission denied while trying to connect to the Docker daemon socket")

    start_cluster("aws", load_topology=load, provisioner=Broken(), operations=ops, lb=lb)
    op = ops.snapshot()
    assert op["status"] == "failed" and "permission denied" in op["error"]


def test_presets_endpoint_survives_a_malformed_preset(web, tmp_path_factory):
    presets = tmp_path_factory.mktemp("presets")
    (presets / "good.yaml").write_text("regions:\n  - {name: us-east-1, provider: aws, nodes: 1}\n", encoding="utf-8")
    (presets / "bad.yaml").write_text("regions:\n  - {name: us-east-1, provider: aws, nodes: 1, zones: 3}\n", encoding="utf-8")
    settings = SimpleNamespace(presets_dir=presets, web_dir=web)
    loader = lambda name: load_preset(presets, name, CAT, default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
    app = create_app(settings=settings, provisioner=Recorder(), collector=SimpleNamespace(latest=dict),
                     operations=Operations(), load_topology=loader, run_in_background=lambda fn: fn())
    out = {p["name"]: p for p in TestClient(app).get("/api/presets").json()}
    assert "zones" in out["bad"]["error"] and out["good"]["regions"][0]["name"] == "us-east-1"


class FakeChaos:
    def __init__(self, result=None, error=None):
        self.calls, self.result, self.error = [], result or {"ok": True, "targets": ["n1"], "warnings": []}, error
        self.latency = None

    def act(self, action, kind, value, **kw):
        self.calls.append((action, kind, value, kw))
        if self.error:
            raise self.error
        return self.result

    def set_latency(self, setting):
        self.latency = setting
        return []


def chaos_client(web, chaos):
    settings = SimpleNamespace(presets_dir=ROOT / "presets", web_dir=web)
    app = create_app(settings=settings, provisioner=Recorder(), collector=SimpleNamespace(latest=dict),
                     operations=Operations(), load_topology=load, chaos=chaos, run_in_background=lambda fn: fn())
    return TestClient(app)




def test_chaos_actions_pass_through_and_report_targets(web):
    chaos = FakeChaos()
    resp = chaos_client(web, chaos).post("/api/chaos", json={"action": "brownout", "kind": "zone", "value": "us-west-2a", "delay_ms": 500})
    assert resp.status_code == 200 and resp.json()["targets"] == ["n1"]
    assert chaos.calls[0][:3] == ("brownout", "zone", "us-west-2a") and chaos.calls[0][3]["delay_ms"] == 500


def test_chaos_beyond_the_survival_goal_needs_confirmation(web):
    chaos = FakeChaos(result={"ok": False, "warning": "2 regions would be down", "targets": []})
    resp = chaos_client(web, chaos).post("/api/chaos", json={"action": "crash", "kind": "region", "value": "us-west-2"})
    assert resp.status_code == 409 and resp.json()["detail"] == {"needs_confirm": True, "warning": "2 regions would be down"}


def test_bad_chaos_requests_are_400(web):
    from controlplane.chaos import ChaosError
    resp = chaos_client(web, FakeChaos(error=ChaosError("no node named 'x'"))).post("/api/chaos", json={"action": "crash", "kind": "node", "value": "x"})
    assert resp.status_code == 400 and "no node" in resp.json()["detail"]


def test_latency_toggle(web):
    chaos = FakeChaos()
    assert chaos_client(web, chaos).post("/api/cluster/latency", json={"setting": "off"}).status_code == 200
    assert chaos.latency == "off"


def test_brownout_parameters_are_bounded(web):
    c = chaos_client(web, FakeChaos())
    for bad in ({"delay_ms": -1}, {"delay_ms": 100000}, {"loss_pct": 150}, {"jitter_ms": -5}):
        assert c.post("/api/chaos", json={"action": "brownout", "kind": "node", "value": "n1", **bad}).status_code == 422


def test_a_bad_preset_still_reconnects_to_the_running_cluster():
    ops, adopted = Operations(), []
    lb = SimpleNamespace(ensure_config=lambda: None)
    prov = SimpleNamespace(adopt_saved=lambda: adopted.append(1) or True)

    def bad(name):
        raise PresetError("unknown preset 'awz'")
    start_cluster("awz", load_topology=bad, provisioner=prov, operations=ops, lb=lb)
    assert adopted == [1]


def test_rebuild_can_switch_the_deployment_model(web):
    seen = []

    def loader(name, model=None):
        seen.append((name, model))
        return load(name)

    settings = SimpleNamespace(presets_dir=ROOT / "presets", web_dir=web)
    app = create_app(settings=settings, provisioner=Recorder(), collector=SimpleNamespace(latest=dict),
                     operations=Operations(), load_topology=loader, run_in_background=lambda fn: fn())
    c = TestClient(app)
    assert c.post("/api/cluster/rebuild", json={"preset": "azure", "model": "single-region"}).status_code == 202
    assert seen[-1] == ("azure", "single-region")
