"""Elasticity, booth, trigger, settings and token endpoints."""
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from controlplane.app import create_app
from controlplane.booth import TriggerError
from controlplane.catalog import load_catalog
from controlplane.elastic import ElasticError
from controlplane.events import Events
from controlplane.operations import OperationBusy, Operations
from controlplane.store import Store
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
CAT = load_catalog(ROOT / "catalog" / "regions.yaml")


def load(name):
    return load_preset(ROOT / "presets", name, CAT, default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)


class FakeElastic:
    def __init__(self, error=None):
        self.calls, self.error = [], error

    def __getattr__(self, name):
        def call(*args, **kw):
            self.calls.append((name, args, kw))
            if self.error:
                raise self.error
        return call


class FakeBooth:
    def __init__(self, error=None):
        self.fired, self.error = [], error

    def fire(self, trigger_id):
        if self.error:
            raise self.error
        self.fired.append(trigger_id)
        return {"ok": True, "trigger": trigger_id, "action": "crash", "kind": "node", "value": "n1"}

    def status(self):
        return {"enabled": False, "auto_heal_s": 45, "heal_in": None, "counters": {}}

    def restart_countdown(self):
        self.countdowns = getattr(self, "countdowns", 0) + 1


@pytest.fixture
def web(tmp_path):
    for d in ("stage", "presenter"):
        (tmp_path / d).mkdir()
        (tmp_path / d / "index.html").write_text(f"<html>{d}</html>", encoding="utf-8")
    return tmp_path


def client(web, tmp_path, *, elastic=None, booth=None, control_token=None, trigger_token=None, presets_dir=None):
    settings = SimpleNamespace(presets_dir=presets_dir or ROOT / "presets", web_dir=web, control_token=control_token,
                               trigger_token=trigger_token, subnet="10.77.0.0/16", max_nodes=15, crdb_version="v26.3.2")
    app = create_app(settings=settings, provisioner=SimpleNamespace(topology=None), collector=SimpleNamespace(latest=dict),
                     operations=Operations(), load_topology=load, elastic=elastic or FakeElastic(),
                     booth=booth or FakeBooth(), store=Store(tmp_path / "data"), events=Events(), catalog=CAT,
                     run_in_background=lambda fn: fn())
    return TestClient(app)


def test_elastic_endpoints_start_operations(web, tmp_path):
    e = FakeElastic()
    c = client(web, tmp_path, elastic=e)
    assert c.post("/api/cluster/nodes", json={"region": "us-east-1"}).status_code == 202
    assert c.delete("/api/cluster/nodes/us-east-1a-n1").status_code == 202
    assert c.post("/api/cluster/regions", json={"name": "eu-west-1", "provider": "aws", "nodes": 3}).status_code == 202
    assert c.delete("/api/cluster/regions/us-west-2").status_code == 202
    assert c.post("/api/cluster/survival", json={"goal": "zone"}).status_code == 202
    assert c.post("/api/cluster/primary", json={"region": "us-west-2"}).status_code == 202
    assert [x[0] for x in e.calls] == ["add_node", "decommission", "add_region", "remove_region", "set_survival", "set_primary"]
    assert e.calls[2][1:] == (("eu-west-1", "aws", 3), {"label": None, "location": None})


def test_elastic_validation_and_busy_errors(web, tmp_path):
    assert client(web, tmp_path, elastic=FakeElastic(ElasticError("last node"))).delete("/api/cluster/nodes/x").status_code == 400
    assert client(web, tmp_path, elastic=FakeElastic(OperationBusy("provision already running"))).post(
        "/api/cluster/nodes", json={"region": "r"}).status_code == 409


def test_triggers_fire_and_report(web, tmp_path):
    b = FakeBooth()
    resp = client(web, tmp_path, booth=b).post("/api/triggers/roach-1")
    assert resp.status_code == 200 and resp.json()["action"] == "crash" and b.fired == ["roach-1"]
    assert client(web, tmp_path, booth=FakeBooth(TriggerError("unknown trigger 'x'"))).post("/api/triggers/x").status_code == 404


def test_trigger_token_is_required_when_set(web, tmp_path):
    c = client(web, tmp_path, trigger_token="s3cret")
    assert c.post("/api/triggers/roach-1").status_code == 401
    assert c.post("/api/triggers/roach-1", headers={"X-Trigger-Token": "s3cret"}).status_code == 200


def test_control_token_guards_mutations_and_presenter_sets_the_cookie(web, tmp_path):
    c = client(web, tmp_path, control_token="tok")
    assert c.get("/api/state").status_code == 200  # the stage stays readable
    assert c.post("/api/cluster/nodes", json={"region": "r"}).status_code == 401
    assert c.get("/presenter?token=tok").status_code == 200
    assert c.post("/api/cluster/nodes", json={"region": "r"}).status_code == 202  # cookie now set
    assert c.post("/api/triggers/roach-1").status_code == 200  # a presenter browser can fire triggers too


def test_settings_booth_and_trigger_map(web, tmp_path):
    c = client(web, tmp_path)
    s = c.get("/api/settings").json()
    assert s["booth"]["auto_heal_s"] == 45 and "roach-1" in s["triggers"]
    assert c.put("/api/booth", json={"enabled": True, "auto_heal_s": 30}).status_code == 200
    assert c.get("/api/settings").json()["booth"] == {"enabled": True, "auto_heal_s": 30}
    new = {"triggers": {"hammer": {"action": "region-outage", "target": "random-region"}}, "keys": {"1": "hammer"}}
    assert c.put("/api/triggers", json=new).status_code == 200
    assert c.get("/api/settings").json()["keys"] == {"1": "hammer"}
    bad = {"triggers": {"x": {"action": "nuke", "target": "all"}}, "keys": {}}
    assert c.put("/api/triggers", json=bad).status_code == 400
    assert c.post("/api/booth/counters/reset").json()["counters"] == {"nodes_crashed": 0, "regions_down": 0}


def test_custom_presets_validate_and_save(web, tmp_path):
    presets = tmp_path / "presets"
    presets.mkdir()
    c = client(web, tmp_path, presets_dir=presets)
    good = "display_name: Acme\nregions:\n  - {name: eastus, provider: azure, nodes: 3}\n"
    resp = c.put("/api/presets/custom/acme", json={"yaml": good})
    assert resp.status_code == 200 and resp.json()["regions"][0]["name"] == "eastus"
    assert (presets / "custom" / "acme.yaml").read_text() == good
    assert c.get("/api/presets/acme/yaml").json()["yaml"] == good
    assert c.put("/api/presets/custom/acme", json={"yaml": "regions: 3"}).status_code == 400
    assert c.put("/api/presets/custom/Bad%20Name", json={"yaml": good}).status_code == 400


def test_presenter_page_is_served(web, tmp_path):
    assert "presenter" in client(web, tmp_path).get("/presenter").text


class FakeWorkload:
    def __init__(self, fail=None):
        self.calls, self.fail = [], fail

    def start(self):
        if self.fail:
            raise self.fail
        self.calls.append("start")

    def stop(self):
        self.calls.append("stop")

    def configure(self, *, workers, rate):
        if workers < 1:
            raise ValueError("workers must be 1-64")
        self.calls.append(("configure", workers, rate))


class FakeRunner:
    def __init__(self):
        self.calls = []

    def catalog(self, topo, latency):
        return [{"id": "time-travel", "unmet": []}]

    def start(self, sid, topo, latency):
        from controlplane.scenarios import ScenarioError
        if sid == "nope":
            raise ScenarioError("unknown scenario 'nope'")
        self.calls.append(("start", sid))

    def next(self):
        self.calls.append("next")

    def stop(self):
        self.calls.append("stop")


def wclient(web, tmp_path, workload, runner):
    settings = SimpleNamespace(presets_dir=ROOT / "presets", web_dir=web, control_token=None, trigger_token=None)
    app = create_app(settings=settings, provisioner=SimpleNamespace(topology=None), collector=SimpleNamespace(latest=dict),
                     operations=Operations(), load_topology=load, workload=workload, scenarios=runner,
                     chaos=SimpleNamespace(latency_setting=lambda: "realistic"), run_in_background=lambda fn: fn())
    return TestClient(app)


def test_workload_endpoints(web, tmp_path):
    w = FakeWorkload()
    c = wclient(web, tmp_path, w, FakeRunner())
    assert c.post("/api/workload/start").status_code == 200
    assert c.put("/api/workload", json={"workers": 8, "rate": 100}).status_code == 200
    assert c.put("/api/workload", json={"workers": 0, "rate": 100}).status_code == 400
    assert c.post("/api/workload/stop").status_code == 200
    assert w.calls == ["start", ("configure", 8, 100), "stop"]
    bad = wclient(web, tmp_path, FakeWorkload(fail=RuntimeError("connection refused")), FakeRunner())
    resp = bad.post("/api/workload/start")
    assert resp.status_code == 503 and "connection refused" in resp.json()["detail"]


def test_scenario_endpoints(web, tmp_path):
    r = FakeRunner()
    c = wclient(web, tmp_path, FakeWorkload(), r)
    assert c.get("/api/scenarios").json()[0]["id"] == "time-travel"
    assert c.post("/api/scenarios/time-travel/start").status_code == 200
    assert c.post("/api/scenarios/next").status_code == 200
    assert c.post("/api/scenarios/stop").status_code == 200
    assert c.post("/api/scenarios/nope/start").status_code == 400
    assert r.calls == [("start", "time-travel"), "next", "stop"]


def test_catalog_lists_regions_per_provider(web, tmp_path):
    cat = client(web, tmp_path).get("/api/catalog").json()
    assert "aws" in cat and any(r["name"] == "eu-west-1" and r["label"] == "Ireland" for r in cat["aws"])


def test_explainers_are_served_with_the_cluster_name(tmp_path):
    settings = SimpleNamespace(presets_dir=ROOT / "presets", web_dir=ROOT / "web", control_token=None, trigger_token=None, preset="aws")
    app = create_app(settings=settings, provisioner=SimpleNamespace(topology=load("azure")), collector=SimpleNamespace(latest=dict),
                     operations=Operations(), load_topology=load, catalog=CAT, run_in_background=lambda fn: fn())
    c = TestClient(app)
    chaos = c.get("/explainer/chaos").text
    assert "eastus" in chaos and "{{DISPLAY_NAME}}" not in chaos
    has_continuum = (ROOT / "web" / "explainers" / "continuum.html").is_file()  # optional, kept out of public branches
    assert c.get("/explainer/continuum").status_code == (200 if has_continuum else 404)
    assert "playlist" in c.get("/explainer/playlist").text.lower()
    assert c.get("/explainer/nope").status_code == 404


def test_explainers_are_optional_and_listed_when_present(tmp_path):
    import shutil
    web = tmp_path / "web"
    shutil.copytree(ROOT / "web", web)
    (web / "explainers" / "continuum.html").unlink(missing_ok=True)  # a checkout without the Continuum explainer
    settings = SimpleNamespace(presets_dir=ROOT / "presets", web_dir=web, control_token=None, trigger_token=None, preset="aws")
    app = create_app(settings=settings, provisioner=SimpleNamespace(topology=None), collector=SimpleNamespace(latest=dict),
                     operations=Operations(), load_topology=load, catalog=CAT, run_in_background=lambda fn: fn())
    c = TestClient(app)
    assert c.get("/explainer/continuum").status_code == 404
    assert [e["name"] for e in c.get("/api/explainers").json()] == ["chaos"]
    assert c.get("/explainer/chaos").status_code == 200


def test_the_chaos_explainer_is_offered_only_where_it_fits():
    settings = SimpleNamespace(presets_dir=ROOT / "presets", web_dir=ROOT / "web", control_token=None, trigger_token=None, preset="aws")
    one = load_preset(ROOT / "presets", "aws", CAT, default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15, model="single-region")
    app = create_app(settings=settings, provisioner=SimpleNamespace(topology=one), collector=SimpleNamespace(latest=dict),
                     operations=Operations(), load_topology=load, catalog=CAT, run_in_background=lambda fn: fn())
    names = [e["name"] for e in TestClient(app).get("/api/explainers").json()]
    assert "chaos" not in names  # it tells a three-region story
