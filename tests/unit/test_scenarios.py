from pathlib import Path
from types import SimpleNamespace

import pytest

from controlplane.catalog import load_catalog
from controlplane.events import Events
from controlplane.operations import Operations
from controlplane.scenarios import Context, Runner, Scenario, ScenarioError, Step
from controlplane.scenarios.library import ALL
from controlplane.topology import load_preset, parse_preset

ROOT = Path(__file__).resolve().parents[2]
CAT = load_catalog(ROOT / "catalog" / "regions.yaml")
KW = dict(default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
AWS = load_preset(ROOT / "presets", "aws", CAT, **KW)
SMALL = load_preset(ROOT / "presets", "small", CAT, **KW)
NINE = {"display_name": "Nine", "regions": [
    {"name": "us-east-1", "provider": "aws", "nodes": 3, "primary": True},
    {"name": "us-west-2", "provider": "aws", "nodes": 3, "secondary": True},
    {"name": "eu-west-1", "provider": "aws", "nodes": 3}]}
AWS9 = parse_preset("aws9", NINE, CAT, **KW)


def ctx_for(topo, **kw):
    prov = SimpleNamespace(topology=topo)
    return Context(provisioner=prov, chaos=kw.get("chaos"), elastic=kw.get("elastic"), workload=kw.get("workload"),
                   operations=Operations(), events=Events(), connect=kw.get("connect", lambda *a, **k: None),
                   lb_host="haproxy", catalog=CAT, sleep=lambda s: None)


def test_requirements_are_reported_per_scenario():
    runner = Runner(ALL, ctx_for(SMALL), run_in_background=lambda f: f())
    by_id = {s["id"]: s for s in runner.catalog(SMALL, "realistic")}
    assert len(by_id) == 12
    assert by_id["time-travel"]["unmet"] == []
    assert "needs 5+ nodes" in by_id["split-brain"]["unmet"]
    assert "needs inter-region latency on" in {s["id"]: s for s in runner.catalog(AWS, "off")}["follower-reads"]["unmet"]


def test_runner_walks_steps_fills_captions_and_resets():
    calls = []
    s = Scenario(id="demo", title="Demo", tier=1, summary="", min_regions=1,
                 prepare=lambda ctx, st: st.update(primary="us-east-1"),
                 reset=lambda ctx, st: calls.append("reset"),
                 steps=(Step("Kill {primary}", "{primary} is down", lambda ctx, st: {"ok": 1}),
                        Step("Boom", "fails", lambda ctx, st: 1 / 0)))
    runner = Runner([s], ctx_for(SMALL), run_in_background=lambda f: f())
    runner.start("demo", SMALL, "realistic")
    assert runner.status()["steps"][0]["caption"] == "us-east-1 is down"
    runner.next()
    st = runner.status()
    assert st["steps"][0]["status"] == "done" and st["steps"][0]["result"] == {"ok": 1}
    runner.next()
    assert runner.status()["steps"][1]["status"] == "failed" and "division" in runner.status()["steps"][1]["error"]
    with pytest.raises(ScenarioError, match="last step"):
        runner.next()
    runner.stop()
    assert calls == ["reset"] and runner.status() is None


def test_unmet_requirements_refuse_to_start():
    runner = Runner(ALL, ctx_for(SMALL), run_in_background=lambda f: f())
    with pytest.raises(ScenarioError, match="5\\+ nodes"):
        runner.start("split-brain", SMALL, "realistic")
    with pytest.raises(ScenarioError, match="unknown"):
        runner.start("nope", SMALL, "realistic")


class RecordingDB:
    """Accepts any SQL, returns plausible rows so scenario code paths run end to end."""
    def __init__(self):
        self.sql = []

    def connect(self, host, **kw):
        db = self

        class Cur:
            description = None

            def execute(self, sql, params=None):
                db.sql.append(sql)
                self.last = sql
                self.description = [1] if sql.lstrip().upper().startswith("SELECT") else None

            def fetchone(self):
                if "CHANGEFEED" in self.last:
                    return (123,)
                if "coordinator_id" in self.last:
                    return (1,)
                if "string_agg" in self.last:
                    return (1, "us-east-1")
                if "cluster_logical_timestamp" in self.last:
                    return ("1760054400000000000.0000000000",)
                return (0,)

            def fetchall(self):
                if "gossip_nodes" in self.last:
                    return [(1, "us-east-1a-n1:26257")]
                if "voting_replicas" in self.last:
                    return [([1],)]
                if "FROM orders AS OF" in self.last:
                    return [(1, "customer 1", 47)]
                return [(0,)]

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        class Conn:
            def cursor(self):
                return Cur()

            def close(self):
                pass

        return Conn()


class FakeChaos:
    def __init__(self):
        self.acts = []

    def act(self, action, kind, value, confirm=False):
        self.acts.append((action, kind, value))
        return {"ok": True, "targets": [value], "warnings": []}

    def recover_all(self):
        self.acts.append(("recover", "all", ""))

    def down_nodes(self):
        return set()

    def flags(self):
        return {}


class FakeWorkload:
    running, rate = True, 250

    def start(self):
        pass

    def mark_failure(self):
        pass

    def check_invariant(self):
        pass

    def status(self):
        return {"recovery_s": 3.0, "money_lost": 0, "history": [{"commits": 5, "retries": 0}], "errors_total": {}, "failed_total": 0}


class FakeElastic:
    def __init__(self, prov):
        self.prov = prov

    def __getattr__(self, name):
        return lambda *a, **k: None


@pytest.mark.parametrize("scenario", ALL, ids=[s.id for s in ALL])
def test_every_scenario_runs_end_to_end_against_fakes(scenario):
    db, chaos = RecordingDB(), FakeChaos()
    prov = SimpleNamespace(topology=AWS9)
    cdc = SimpleNamespace(reset=lambda: None, status=lambda: {"events": 10, "events_s": 5, "resolved_lag_s": 1.0, "max_gap_s": 4.0},
                          resolved_at=10**12)
    ctx = Context(provisioner=prov, chaos=chaos, elastic=FakeElastic(prov), workload=FakeWorkload(), operations=Operations(),
                  events=Events(), connect=db.connect, lb_host="haproxy", catalog=CAT, cdc=cdc, sleep=lambda s: None)
    runner = Runner(ALL, ctx, run_in_background=lambda f: f())
    runner.start(scenario.id, AWS9, "realistic")
    for _ in scenario.steps:
        runner.next()
    failed = [s for s in runner.status()["steps"] if s["status"] != "done"]
    assert not failed, failed
    assert "{" not in runner.status()["steps"][0]["caption"]
    runner.stop()


def test_a_step_still_running_after_stop_cannot_touch_the_next_scenario():
    pending = []
    slow = Scenario(id="slow", title="Slow", tier=1, summary="", steps=(Step("go", "c", lambda ctx, st: {"late": 1}),))
    other = Scenario(id="other", title="Other", tier=1, summary="", steps=(Step("a", "b", None), Step("c", "d", None)))
    runner = Runner([slow, other], ctx_for(SMALL), run_in_background=lambda f: pending.append(f))
    runner.start("slow", SMALL, "realistic")
    runner.next()                      # queued, not finished
    runner.stop()
    runner.start("other", SMALL, "realistic")
    for f in pending:
        f()                            # the old step finishes now
    st = runner.status()
    assert st["id"] == "other" and all(s["result"] is None for s in st["steps"])


def test_starting_a_scenario_resets_the_previous_one_first():
    calls = []
    a = Scenario(id="a", title="A", tier=1, summary="", steps=(Step("x", "y", None),), reset=lambda ctx, st: calls.append("reset-a"))
    b = Scenario(id="b", title="B", tier=1, summary="", steps=(Step("x", "y", None),), prepare=lambda ctx, st: calls.append("prep-b"))
    runner = Runner([a, b], ctx_for(SMALL), run_in_background=lambda f: f())
    runner.start("a", SMALL, "realistic")
    runner.start("b", SMALL, "realistic")
    assert calls == ["reset-a", "prep-b"]


class FlakyDB(RecordingDB):
    def __init__(self, errors):
        super().__init__()
        self.errors = list(errors)

    def connect(self, host, **kw):
        conn = super().connect(host, **kw)
        db = self
        real_cursor = conn.cursor

        def cursor():
            cur = real_cursor()
            execute = cur.execute

            def flaky(sql, params=None):
                if db.errors:
                    err = db.errors.pop(0)
                    if err:
                        raise err
                return execute(sql, params)
            cur.execute = flaky
            return cur
        conn.cursor = cursor
        return conn


class Coded(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.sqlstate = code


def test_context_retries_transient_errors_for_idempotent_work_only():
    db = FlakyDB([Coded("40001"), Coded("08006")])
    ctx = Context(provisioner=SimpleNamespace(topology=AWS), chaos=FakeChaos(), elastic=None, workload=None,
                  operations=Operations(), events=Events(), connect=db.connect, lb_host="haproxy", sleep=lambda s: None)
    ctx.run("CREATE TABLE IF NOT EXISTS t (id UUID PRIMARY KEY)")
    assert db.sql == ["CREATE TABLE IF NOT EXISTS t (id UUID PRIMARY KEY)"]
    db.errors = [Coded("40003")]
    with pytest.raises(Coded):
        ctx.query("CREATE CHANGEFEED FOR TABLE accounts", one=True, idempotent=False)


def test_zone_vs_region_asks_for_a_third_primary_node_on_a_six_node_cluster():
    six = load_preset(ROOT / "presets", "aws", CAT, **KW)
    by_id = {s["id"]: s for s in Runner(ALL, ctx_for(six), run_in_background=lambda f: f()).catalog(six, "realistic")}
    assert any("3 nodes in us-east-1" in u for u in by_id["zone-vs-region"]["unmet"])
    assert by_id["survive-region"]["unmet"] == []


def test_single_region_gets_the_zone_outage_scenario_and_not_the_region_ones():
    one = load_preset(ROOT / "presets", "aws", CAT, model="single-region", **KW)
    by_id = {s["id"]: s for s in Runner(ALL, ctx_for(one), run_in_background=lambda f: f()).catalog(one, "realistic")}
    assert by_id["survive-zone"]["unmet"] == []
    assert by_id["survive-region"]["unmet"] and by_id["regional-by-row"]["unmet"]
    assert by_id["time-travel"]["unmet"] == [] and by_id["elastic-scale"]["unmet"] == []


def test_write_skew_withdrawal_backs_off_between_retries_and_always_reports_an_outcome():
    import threading
    from controlplane.scenarios.library import _withdraw

    class Retry(Exception):
        sqlstate = "40001"

    class Cur:
        def __init__(self, fails):
            self.fails, self.last = fails, ""

        def execute(self, sql, params=None):
            self.last = sql
            if sql.startswith("SELECT") and self.fails:
                self.fails -= 1
                raise Retry("ReadWithinUncertaintyIntervalError: restart transaction")

        def fetchone(self):
            return (0,)  # the other withdrawal already took the money

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    sleeps = []
    for fails, expected in ((6, "declined"), (99, "gave up")):
        cur = Cur(fails)
        conn = SimpleNamespace(cursor=lambda cur=cur: cur, close=lambda: None)
        ctx = SimpleNamespace(connect=lambda **kw: conn, sleep=sleeps.append)
        out = {}
        _withdraw(ctx, "SERIALIZABLE", "savings", threading.Barrier(1), out)
        assert out["savings"].startswith(expected), out
    assert sleeps and all(s > 0 for s in sleeps)  # never retries back to back
