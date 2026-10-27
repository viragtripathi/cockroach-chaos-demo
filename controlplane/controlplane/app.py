"""HTTP API and static pages for the control plane."""
from __future__ import annotations

import asyncio
import hmac
import json
import logging
import threading
from typing import Callable

import yaml
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .booth import TRIGGER_ACTIONS, TRIGGER_TARGETS, TriggerError
from .chaos import ChaosError
from .elastic import ElasticError
from .explainers import render_chaos
from .operations import OperationBusy
from .scenarios import ScenarioError
from .topology import NAME_RE, PresetError, list_presets, parse_preset

log = logging.getLogger(__name__)
COOKIE = "crdb_control"


class RebuildRequest(BaseModel):
    preset: str
    model: str | None = None  # deployment model; None keeps the configured default


class ChaosRequest(BaseModel):
    action: str
    kind: str
    value: str = ""
    confirm: bool = False
    delay_ms: int = Field(700, ge=0, le=10000)
    jitter_ms: int | None = Field(None, ge=0, le=10000)
    loss_pct: float = Field(2.0, ge=0, le=100)


class LatencyRequest(BaseModel):
    setting: str


class AddNodeRequest(BaseModel):
    region: str


class AddRegionRequest(BaseModel):
    name: str
    provider: str = "custom"
    nodes: int = 3
    label: str | None = None
    location: list[float] | None = None


class SurvivalRequest(BaseModel):
    goal: str


class PrimaryRequest(BaseModel):
    region: str


class BoothRequest(BaseModel):
    enabled: bool
    auto_heal_s: int = 45


class TriggerMapRequest(BaseModel):
    triggers: dict[str, dict]
    keys: dict[str, str] = {}


class PresetYaml(BaseModel):
    yaml: str


class WorkloadRequest(BaseModel):
    workers: int
    rate: int
    cross_region_pct: int = 10  # share of Payments transfers sent to an account in another region


def _same(a: str | None, b: str | None) -> bool:
    return bool(a) and bool(b) and hmac.compare_digest(a.encode(), b.encode())


def _background(fn: Callable[[], None]) -> None:
    threading.Thread(target=fn, daemon=True).start()


def start_cluster(preset: str, *, load_topology, provisioner, operations, lb) -> None:
    """Startup work, run on a background thread: never let a bad preset crash-loop the container."""
    def fail(message: str) -> None:
        current = operations.snapshot()
        if current and current["status"] == "running":
            return  # that operation already owns the outcome
        op = operations.start("provision")
        operations.finish(op, error=message)

    try:
        lb.ensure_config()
        topo = load_topology(preset)
    except PresetError as e:
        # A typo in PRESET must not hide a cluster that is already running: reconnect to it and say why.
        try:
            if provisioner.adopt_saved():
                log.warning("PRESET could not be loaded (%s); reconnected to the existing cluster", e)
                return
        except Exception as adopt_error:
            log.warning("could not reconnect to the existing cluster: %s", adopt_error)
        fail(str(e))
        return
    except Exception as e:  # e.g. the runtime socket is unreachable
        fail(f"Startup failed: {e}")
        return
    try:
        provisioner.ensure(topo)
    except Exception as e:  # ensure() records its own operations; this catches failures before one starts
        fail(f"Startup failed: {e}")


def create_app(*, settings, provisioner, collector, operations, load_topology, chaos=None, elastic=None, booth=None,
               store=None, events=None, catalog=None, workload=None, scenarios=None, system_info: Callable[[], dict] | None = None,
               run_in_background: Callable[[Callable[[], None]], None] | None = None) -> FastAPI:
    run_bg = run_in_background or _background
    app = FastAPI(title="CockroachDB chaos control plane")
    control_token = getattr(settings, "control_token", None)
    trigger_token = getattr(settings, "trigger_token", None)

    def authorized(request: Request) -> bool:
        if not control_token:
            return True
        supplied = request.headers.get("x-control-token") or request.cookies.get(COOKIE)
        return _same(supplied, control_token)

    def require_control(request: Request) -> None:
        if not authorized(request):
            raise HTTPException(status_code=401, detail="control token required: open /presenter?token=…")

    guarded = [Depends(require_control)]

    def publish(text: str, level: str = "info") -> None:
        if events is not None:
            events.publish(text, level)

    def elastic_call(fn, *args, event: str, **kw):
        try:
            fn(*args, **kw)
        except ElasticError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except OperationBusy as e:
            raise HTTPException(status_code=409, detail=str(e))
        publish(event)
        return {"accepted": True}

    @app.get("/api/state")
    def state():
        return collector.latest()

    @app.get("/api/events")
    async def event_stream(request: Request, once: bool = False):
        async def stream():
            while not await request.is_disconnected():
                yield f"data: {json.dumps(collector.latest())}\n\n"
                if once:  # single event; used by tests and quick curl checks
                    break
                await asyncio.sleep(1.0)
        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})

    @app.get("/api/presets")
    def presets():
        out = []
        for name in list_presets(settings.presets_dir):
            try:
                t = load_topology(name)
            except PresetError as e:
                out.append({"name": name, "error": str(e)})
                continue
            out.append({"name": name, "display_name": t.display_name, "survival_goal": t.survival_goal,
                        "regions": [{"name": r.name, "provider": r.provider, "nodes": r.nodes} for r in t.regions],
                        "warnings": list(t.warnings)})
        return out

    @app.post("/api/cluster/rebuild", status_code=202, dependencies=guarded)
    def rebuild(req: RebuildRequest):
        try:
            topo = load_topology(req.preset, model=req.model) if req.model else load_topology(req.preset)
        except PresetError as e:
            raise HTTPException(status_code=400, detail=str(e))
        current = operations.snapshot()
        if current and current["status"] == "running":
            raise HTTPException(status_code=409, detail=f"{current['kind']} already running")

        def work():
            try:
                provisioner.rebuild(topo)
            except OperationBusy:
                pass  # lost a race with another request; that operation reports itself
        run_bg(work)
        return {"accepted": True, "preset": req.preset, "model": topo.model}

    @app.post("/api/chaos", dependencies=guarded)
    def run_chaos(req: ChaosRequest):
        try:
            result = chaos.act(req.action, req.kind, req.value, confirm=req.confirm,
                               delay_ms=req.delay_ms, jitter_ms=req.jitter_ms, loss_pct=req.loss_pct)
        except ChaosError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if not result.get("ok"):
            raise HTTPException(status_code=409, detail={"needs_confirm": True, "warning": result.get("warning")})
        label = "everything" if req.kind == "all" else req.value
        publish(f"{req.action.capitalize()} · {label}", "good" if req.action in ("recover", "start", "heal") else "bad")
        return result

    @app.post("/api/cluster/latency", dependencies=guarded)
    def latency(req: LatencyRequest):
        try:
            warnings = chaos.set_latency(req.setting)
        except ChaosError as e:
            raise HTTPException(status_code=400, detail=str(e))
        publish(f"Inter-region latency: {req.setting}")
        return {"ok": True, "warnings": warnings}

    # -- elasticity --

    @app.post("/api/cluster/nodes", status_code=202, dependencies=guarded)
    def add_node(req: AddNodeRequest):
        return elastic_call(elastic.add_node, req.region, event=f"Adding a node in {req.region}")

    @app.delete("/api/cluster/nodes/{name}", status_code=202, dependencies=guarded)
    def decommission(name: str):
        return elastic_call(elastic.decommission, name, event=f"Decommissioning {name}")

    @app.post("/api/cluster/regions", status_code=202, dependencies=guarded)
    def add_region(req: AddRegionRequest):
        return elastic_call(elastic.add_region, req.name, req.provider, req.nodes, event=f"Adding region {req.name}",
                            label=req.label, location=req.location)

    @app.delete("/api/cluster/regions/{name}", status_code=202, dependencies=guarded)
    def remove_region(name: str):
        return elastic_call(elastic.remove_region, name, event=f"Removing region {name}")

    @app.post("/api/cluster/survival", status_code=202, dependencies=guarded)
    def survival(req: SurvivalRequest):
        return elastic_call(elastic.set_survival, req.goal, event=f"Survival goal → {req.goal.upper()}")

    @app.post("/api/cluster/primary", status_code=202, dependencies=guarded)
    def primary(req: PrimaryRequest):
        return elastic_call(elastic.set_primary, req.region, event=f"Primary region → {req.region}")

    # -- booth and triggers --

    @app.post("/api/triggers/{trigger_id}")
    def fire(trigger_id: str, request: Request):
        if trigger_token and not (_same(request.headers.get("x-trigger-token"), trigger_token) or
                                  (control_token and authorized(request))):
            raise HTTPException(status_code=401, detail="trigger token required")
        if not trigger_token and not authorized(request):
            raise HTTPException(status_code=401, detail="control token required")
        try:
            return booth.fire(trigger_id)
        except TriggerError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except ChaosError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/api/settings")
    def get_settings():
        return store.all()

    @app.put("/api/booth", dependencies=guarded)
    def put_booth(req: BoothRequest):
        if not 5 <= req.auto_heal_s <= 3600:
            raise HTTPException(status_code=400, detail="auto_heal_s must be between 5 and 3600")
        was_on = store.get("booth")["enabled"]
        store.set("booth", {"enabled": req.enabled, "auto_heal_s": req.auto_heal_s})
        if req.enabled and not was_on:
            booth.restart_countdown()
        publish("Booth mode on" if req.enabled else "Booth mode off")
        return booth.status()

    @app.post("/api/booth/counters/reset", dependencies=guarded)
    def reset_counters():
        store.set("counters", {"nodes_crashed": 0, "regions_down": 0})
        return {"counters": store.get("counters")}

    @app.put("/api/triggers", dependencies=guarded)
    def put_triggers(req: TriggerMapRequest):
        for tid, spec in req.triggers.items():
            if not NAME_RE.match(tid) or spec.get("action") not in TRIGGER_ACTIONS or \
                    spec.get("target", "random-safe") not in TRIGGER_TARGETS:
                raise HTTPException(status_code=400, detail=f"trigger {tid}: needs a known action and target")
        for key, tid in req.keys.items():
            if key not in "123456789" or len(key) != 1 or tid not in req.triggers:
                raise HTTPException(status_code=400, detail=f"key {key}: must be 1-9 and map to a defined trigger")
        store.set("triggers", req.triggers)
        store.set("keys", req.keys)
        return {"ok": True}

    # -- custom presets --

    @app.get("/api/presets/{name}/yaml")
    def preset_yaml(name: str):
        if not NAME_RE.match(name):
            raise HTTPException(status_code=400, detail="bad preset name")
        for candidate in (settings.presets_dir / f"{name}.yaml", settings.presets_dir / "custom" / f"{name}.yaml"):
            if candidate.is_file():
                return {"name": name, "yaml": candidate.read_text(encoding="utf-8")}
        raise HTTPException(status_code=404, detail=f"unknown preset {name!r}")

    @app.put("/api/presets/custom/{name}", dependencies=guarded)
    def save_preset(name: str, req: PresetYaml):
        if not NAME_RE.match(name):
            raise HTTPException(status_code=400, detail="preset names are lowercase letters, digits and hyphens")
        try:
            topo = parse_preset(name, yaml.safe_load(req.yaml), catalog, default_version=settings.crdb_version,
                                subnet=settings.subnet, max_nodes=settings.max_nodes)
        except yaml.YAMLError as e:
            raise HTTPException(status_code=400, detail=f"invalid YAML: {e}")
        except PresetError as e:
            raise HTTPException(status_code=400, detail=str(e))
        target = settings.presets_dir / "custom" / f"{name}.yaml"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(req.yaml, encoding="utf-8")
        return {"name": name, "survival_goal": topo.survival_goal, "warnings": list(topo.warnings),
                "regions": [{"name": r.name, "provider": r.provider, "nodes": r.nodes} for r in topo.regions]}

    # -- workload --

    @app.post("/api/workload/start", dependencies=guarded)
    def workload_start():
        try:
            workload.start()
        except Exception as e:  # cluster unreachable, setup failed
            raise HTTPException(status_code=503, detail=f"could not start payments: {e}")
        publish("Payments workload started")
        return {"ok": True}

    @app.post("/api/workload/stop", dependencies=guarded)
    def workload_stop():
        workload.stop()
        publish("Payments workload stopped")
        return {"ok": True}

    @app.put("/api/workload", dependencies=guarded)
    def workload_configure(req: WorkloadRequest):
        try:
            workload.configure(workers=req.workers, rate=req.rate, cross_region_pct=req.cross_region_pct)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"ok": True}

    # -- scenarios --

    def _latency():
        try:
            return chaos.latency_setting() if chaos else None
        except ChaosError:
            return None

    @app.get("/api/scenarios")
    def scenario_list():
        return scenarios.catalog(provisioner.topology, _latency())

    @app.get("/api/scenarios/current")
    def scenario_current():
        return scenarios.status()

    @app.post("/api/scenarios/{scenario_id}/start", dependencies=guarded)
    def scenario_start(scenario_id: str):
        try:
            scenarios.start(scenario_id, provisioner.topology, _latency())
        except ScenarioError as e:
            raise HTTPException(status_code=400, detail=str(e))
        return {"ok": True}

    @app.post("/api/scenarios/next", dependencies=guarded)
    def scenario_next():
        try:
            scenarios.next()
        except ScenarioError as e:
            raise HTTPException(status_code=409, detail=str(e))
        return {"ok": True}

    @app.post("/api/scenarios/stop", dependencies=guarded)
    def scenario_stop():
        scenarios.stop()
        return {"ok": True}

    @app.get("/api/catalog")
    def region_catalog():
        return {p: [{"name": r.name, "label": r.label} for r in catalog.regions_for(p)] for p in catalog.providers()}

    @app.get("/api/system")
    def system(request: Request):
        """Presenter-only facts (real endpoints, connection strings). Never rendered on the stage."""
        return {"authorized": authorized(request), "token_required": bool(control_token),
                **(system_info() if system_info else {})}

    @app.get("/")
    def stage():
        return FileResponse(settings.web_dir / "stage" / "index.html")

    @app.get("/presenter")
    def presenter(response: Response, token: str | None = None):
        page = FileResponse(settings.web_dir / "presenter" / "index.html")
        if control_token and _same(token, control_token):
            page.set_cookie(COOKIE, token, httponly=True, samesite="strict", max_age=7 * 24 * 3600)
        return page

    explainer_lengths = {"chaos": 133, "continuum": 162}  # seconds; the playlist alternates the ones present

    @app.get("/api/explainers")
    def explainers():
        topo = provisioner.topology
        fits = {"chaos": topo is None or len(topo.regions) >= 3}  # the chaos explainer tells a three-region story
        return [{"name": n, "seconds": s} for n, s in explainer_lengths.items()
                if fits.get(n, True) and (settings.web_dir / "explainers" / f"{n}.html").is_file()]

    @app.get("/explainer/{name}")
    def explainer(name: str):
        if name == "playlist":
            return FileResponse(settings.web_dir / "explainers" / "playlist.html")
        path = settings.web_dir / "explainers" / f"{name}.html"
        if name not in explainer_lengths or not path.is_file():
            raise HTTPException(status_code=404, detail="unknown explainer")
        html = path.read_text(encoding="utf-8")
        topo = provisioner.topology
        if topo is None:
            try:
                display = load_topology(settings.preset).display_name
            except Exception:
                display = "CockroachDB"
        else:
            display = topo.display_name
        if name == "chaos":
            html = render_chaos(html, topo, catalog, display)
        return HTMLResponse(html, headers={"Cache-Control": "no-cache"})

    app.mount("/static", StaticFiles(directory=settings.web_dir), name="static")
    return app
