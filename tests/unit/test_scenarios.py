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
STRETCH = load_preset(ROOT / "presets", "stretch", CAT, **KW)
RESIDENCY = load_preset(ROOT / "presets", "residency", CAT, **KW)


def ctx_for(topo, **kw):
    prov = SimpleNamespace(topology=topo)
    return Context(provisioner=prov, chaos=kw.get("chaos"), elastic=kw.get("elastic"), workload=kw.get("workload"),
                   operations=Operations(), events=Events(), connect=kw.get("connect", lambda *a, **k: None),
                   lb_host="haproxy", catalog=CAT, sleep=lambda s: None)


def test_requirements_are_reported_per_scenario():
    runner = Runner(ALL, ctx_for(SMALL), run_in_background=lambda f: f())
    by_id = {s["id"]: s for s in runner.catalog(SMALL, "realistic")}
    assert len(by_id) == 14
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
    def __init__(self, live=()):
        self.sql = []
        self.live_names = list(live)
        # liveness rows (address, is_live, seconds since the node started) and store health sums
        # (unavailable ranges, ranges missing a copy, copies waiting for a snapshot): a whole, settled cluster
        self.nodes = lambda: [(f"{n}:26257", True, 60.0) for n in self.live_names]
        self.store_health = lambda: (0, 0, 0)
        # node ids (1-based, in live_names order) holding one voter in each region: REGION survival placement
        self.spread_voters = lambda: list({n.rsplit("-", 1)[0][:-1]: i + 1 for i, n in enumerate(self.live_names)}.values())

    def connect(self, host, **kw):
        db = self

        class Cur:
            description = None

            def execute(self, sql, params=None):
                db.sql.append(sql)
                self.last = sql
                self.description = [1] if sql.lstrip().upper().startswith("SELECT") else None

            def fetchone(self):
                if "queue.raftsnapshot.pending" in self.last:
                    return db.store_health()
                if "SHOW RANGE FROM TABLE residency.customers FOR ROW" in self.last:  # every copy in the row's home region
                    home = self.last.split("FOR ROW ('")[1].split("'")[0]
                    return (100 + len(home), [f"region={home},zone={home}{z}" for z in "abc"])
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
                if "gossip_nodes" in self.last and "is_live" in self.last:
                    return db.nodes()
                if "gossip_nodes" in self.last:
                    return [(i + 1, f"{n}:26257") for i, n in enumerate(db.live_names)] or [(1, "us-east-1a-n1:26257")]
                if "voting_replicas" in self.last and "range_id" in self.last:  # REGION placement: one voter per region
                    return [(100, db.spread_voters())]
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
    running, rate, cross_region_pct = True, 250, 10

    def set_cross_region(self, pct):
        self.cross_region_pct = pct

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
    topo = STRETCH if scenario.needs_tie_breaker else RESIDENCY if scenario.id == "data-domicile" else AWS9
    prov.topology = topo
    db.live_names = [n.name for n in topo.nodes]
    runner = Runner(ALL, ctx, run_in_background=lambda f: f())
    runner.start(scenario.id, topo, "realistic")
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


def test_zone_vs_region_needs_three_nodes_in_every_region_that_homes_payments():
    # under ZONE survival each region's payments keep all three voters at home, so every region needs three nodes;
    # a third node in the primary alone leaves the other regions' partitions unplaceable
    six = load_preset(ROOT / "presets", "aws", CAT, **KW)
    by_id = {s["id"]: s for s in Runner(ALL, ctx_for(six), run_in_background=lambda f: f()).catalog(six, "realistic")}
    assert any("3 nodes in every region" in u for u in by_id["zone-vs-region"]["unmet"])
    assert by_id["survive-region"]["unmet"] == []
    primary_plus_one = parse_preset("p1", {"regions": [{"name": "us-east-1", "provider": "aws", "nodes": 3, "primary": True},
                                                       {"name": "us-west-2", "provider": "aws", "nodes": 2},
                                                       {"name": "eu-west-1", "provider": "aws", "nodes": 2}]}, CAT, **KW)
    assert any("us-west-2 has 2" in u for u in {s["id"]: s for s in Runner(ALL, ctx_for(primary_plus_one), run_in_background=lambda f: f())
                                                .catalog(primary_plus_one, "realistic")}["zone-vs-region"]["unmet"])
    assert {s["id"]: s for s in Runner(ALL, ctx_for(RESIDENCY), run_in_background=lambda f: f())
            .catalog(RESIDENCY, "realistic")}["zone-vs-region"]["unmet"] == []


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


def _voter_ctx(ranges, nodes=((1, "us-east-1"), (2, "us-east-1"), (3, "us-west-2"), (4, "us-west-2"))):
    import itertools
    topo = SimpleNamespace(nodes=[SimpleNamespace(name=f"n{i}", region=r) for i, r in nodes])
    ticks = itertools.count(step=100.0)

    def query(sql, host=None, **kw):
        if "gossip_nodes" in sql:
            return [(i, f"n{i}:26257") for i, _ in nodes]
        if "range_id" in sql:
            return [(100 + k, list(v)) for k, v in enumerate(ranges)]
        return [(list(v),) for v in ranges]

    return SimpleNamespace(topo=topo, query=query, clock=lambda: next(ticks), sleep=lambda s: None)


NINE_NODES = ((1, "us-east-1"), (2, "us-east-1"), (3, "us-east-1"), (4, "eu-west-1"), (5, "eu-west-1"), (6, "eu-west-1"),
              (7, "ap-southeast-1"), (8, "ap-southeast-1"), (9, "ap-southeast-1"))


def test_region_survival_placement_means_every_range_survives_losing_any_region():
    from controlplane.scenarios import library as L
    assert L._wait_spread(_voter_ctx([(1, 2, 4, 5, 7), (4, 5, 1, 7, 8)], NINE_NODES)) >= 0
    with pytest.raises(ScenarioError, match="spreading"):  # ZONE placement: every voter in one region
        L._wait_spread(_voter_ctx([(1, 2, 3)], NINE_NODES))
    with pytest.raises(ScenarioError, match="spreading"):  # 3 of 5 in one region: losing it loses the majority
        L._wait_spread(_voter_ctx([(1, 2, 3, 4, 7)], NINE_NODES))


def test_the_region_step_waits_for_the_voters_not_the_lagging_counter():
    """Right after the ALTER the under-replicated counter still reads 0 (it refreshes every 10 s): the step must not
    take that for "rebalanced" while every voter is still in one region."""
    from controlplane.scenarios import library as L
    ctx = _voter_ctx([(1, 2, 3)], NINE_NODES)
    voters = ctx.query
    ctx.query = lambda sql, host=None, one=False, **kw: (0,) if "underreplicated" in sql else voters(sql, host)
    ctx.topo.survival_goal = "region"  # the switch itself is done; placement is what's left
    ctx.workload = SimpleNamespace(cross_region_pct=0, set_cross_region=lambda pct: None)
    with pytest.raises(ScenarioError, match="spreading"):
        L._region_step(ctx, {"cross_region_pct": 10})


def test_zone_vs_region_pauses_cross_region_transfers_for_the_zone_half_only():
    """Under ZONE survival a transfer to a customer in the lost region waits for it, and each Payments worker sends one
    transfer at a time: left on, those transfers hold up every region's payments, not only the lost region's."""
    workload, prov = FakeWorkload(), SimpleNamespace(topology=RESIDENCY)
    ctx = Context(provisioner=prov, chaos=FakeChaos(), elastic=FakeElastic(prov), workload=workload, operations=Operations(),
                  events=Events(), connect=RecordingDB(live=[n.name for n in RESIDENCY.nodes]).connect, lb_host="haproxy",
                  catalog=CAT, sleep=lambda s: None)
    runner = Runner(ALL, ctx, run_in_background=lambda f: f())
    runner.start("zone-vs-region", RESIDENCY, "realistic")
    runner.next()  # switch to ZONE survival
    assert workload.cross_region_pct == 0
    assert runner.status()["steps"][0]["result"]["Cross-region transfers"].startswith("paused")
    for _ in range(3):  # take down the primary, recover it, switch to REGION survival
        runner.next()
    assert workload.cross_region_pct == 10
    assert runner.status()["steps"][3]["result"]["Cross-region transfers"] == "back on: 10%"
    assert all(s["status"] == "done" for s in runner.status()["steps"][:4])
    runner.stop()

    runner.start("zone-vs-region", RESIDENCY, "realistic")
    runner.next()
    runner.stop()  # ended during the ZONE half: the share comes back anyway
    assert workload.cross_region_pct == 10


def test_zone_survival_placement_means_one_region_per_range_not_one_region_for_all():
    from controlplane.scenarios import library as L
    # REGIONAL BY ROW under ZONE survival: each partition keeps its voters in its own home region.
    assert L._wait_voters(_voter_ctx([(1, 2), (3, 4)]), "us-east-1") >= 0
    with pytest.raises(ScenarioError, match="still moving"):
        L._wait_voters(_voter_ctx([(1, 3), (2, 4)]), "us-east-1")


def test_the_zone_stall_reports_the_primarys_payments_stalled_while_other_regions_pay():
    from controlplane.scenarios import library as L
    acted = []
    status = {"history": [{"ts": t, "commits": 4} for t in range(10)],
              "by_home": {"us-west-2": {"p50_ms": 70.0, "p95_ms": 90.0, "tps": 40}, "eu-west-1": {"p50_ms": 95.0, "p95_ms": 120.0, "tps": 30}}}
    ctx = SimpleNamespace(chaos=SimpleNamespace(act=lambda *a, **k: acted.append(a)),
                          workload=SimpleNamespace(mark_failure=lambda: None, status=lambda: status), sleep=lambda s: None)
    out = L._stall(ctx, {"primary": "us-east-1"})
    assert acted == [("outage", "region", "us-east-1")]
    assert out["Payments homed in us-east-1"] == "stalled until the region returns"
    assert out["Other regions"] == "keep paying: 70/s"


def test_the_stretch_scenario_needs_a_tie_breaker():
    runner = Runner(ALL, ctx_for(AWS), run_in_background=lambda f: f())
    assert "needs a tie-breaker region: build the stretch preset" in {s["id"]: s for s in runner.catalog(AWS, "realistic")}["stretch-cloud"]["unmet"]
    assert {s["id"]: s for s in runner.catalog(STRETCH, "realistic")}["stretch-cloud"]["unmet"] == []


def test_the_stretch_scenario_loses_a_whole_cloud_then_only_the_tie_breaker():
    chaos, prov = FakeChaos(), SimpleNamespace(topology=STRETCH)
    db = RecordingDB(live=[n.name for n in STRETCH.nodes])
    ctx = Context(provisioner=prov, chaos=chaos, elastic=FakeElastic(prov), workload=FakeWorkload(), operations=Operations(),
                  events=Events(), connect=db.connect, lb_host="haproxy", catalog=CAT, sleep=lambda s: None)
    runner = Runner(ALL, ctx, run_in_background=lambda f: f())
    runner.start("stretch-cloud", STRETCH, "realistic")
    steps = [s for s in ALL if s.id == "stretch-cloud"][0].steps
    for _ in steps:
        runner.next()
    status = runner.status()
    assert all(s["status"] == "done" for s in status["steps"]), status["steps"]
    outages = [a for a in chaos.acts if a[0] in ("outage", "recover") and a[1] == "region"]
    assert outages == [("outage", "region", "us-east-1"), ("recover", "region", "us-east-1"),
                       ("outage", "region", "us-east4"), ("recover", "region", "us-east4")]
    captions = " ".join(s["caption"] for s in status["steps"])
    assert "AWS" in captions and "us-east4" in captions and "{" not in captions
    runner.stop()


def _stretch_ctx(db, chaos, clock=None):
    prov = SimpleNamespace(topology=STRETCH)
    t = [0.0]
    return Context(provisioner=prov, chaos=chaos, elastic=FakeElastic(prov), workload=FakeWorkload(), operations=Operations(),
                   events=Events(), connect=db.connect, lb_host="haproxy", catalog=CAT,
                   sleep=lambda s: t.__setitem__(0, t[0] + s), clock=lambda: t[0])


def test_bringing_a_cloud_back_waits_until_it_is_live_and_caught_up():
    """The tie-breaker step comes next, and losing it is only harmless once AWS votes again with current copies.
    Restarting the containers takes a second; the nodes rejoin a few seconds later, and CockroachDB's store status
    (written every 10 s) shows the missing copies back only after that."""
    names = [n.name for n in STRETCH.nodes]
    aws = {n.name for n in STRETCH.nodes if n.region == "us-east-1"}
    rejoining = iter([
        [(f"{n}:26257", n not in aws, 60.0) for n in names],  # AWS not live yet
        [(f"{n}:26257", n not in aws, 60.0) for n in names],
        [(f"{n}:26257", True, 3.0 if n in aws else 60.0) for n in names],  # live, but its store status isn't in yet
    ])
    db = RecordingDB(live=names)
    db.nodes = lambda: next(rejoining, [(f"{n}:26257", True, 60.0) for n in names])
    health = iter([(0, 106, 0), (0, 0, 4)])  # copies missing, then copies waiting for a snapshot
    db.store_health = lambda: next(health, (0, 0, 0))
    chaos = FakeChaos()
    runner = Runner(ALL, _stretch_ctx(db, chaos), run_in_background=lambda f: f())
    runner.start("stretch-cloud", STRETCH, "realistic")
    for _ in range(4):  # start payments, take down us-east-1, measure, bring us-east-1 back
        runner.next()
    back = runner.status()["steps"][3]
    assert back["status"] == "done", back
    assert back["result"]["Recovered"] == "us-east-1" and back["result"]["Caught up in"].endswith(" s"), back["result"]
    assert next(rejoining, None) is None and next(health, None) is None  # it kept checking until the cluster was whole


def test_the_tie_breaker_stays_up_until_the_rest_of_the_cluster_is_whole():
    """If AWS is still rejoining, losing the tie-breaker would leave Azure alone with 2 of 5 copies: no majority."""
    names = [n.name for n in STRETCH.nodes]
    aws = {n.name for n in STRETCH.nodes if n.region == "us-east-1"}
    db = RecordingDB(live=names)
    db.nodes = lambda: [(f"{n}:26257", n not in aws, 60.0) for n in names]  # AWS never rejoins
    chaos = FakeChaos()
    runner = Runner(ALL, _stretch_ctx(db, chaos), run_in_background=lambda f: f())
    runner.start("stretch-cloud", STRETCH, "realistic")
    for _ in range(5):  # ... bring us-east-1 back, take down us-east4
        runner.next()
    steps = runner.status()["steps"]
    assert steps[3]["status"] == "failed" and "us-east-1a-n1" in steps[3]["error"], steps[3]
    assert steps[4]["status"] == "failed" and "majority" in steps[4]["error"], steps[4]
    assert ("outage", "region", "us-east4") not in chaos.acts


def test_retries_are_counted_from_the_failure_not_over_the_last_minute():
    """After a cloud comes back, the next failure's result must not inherit the retries of the one before."""
    from controlplane.scenarios.library import _wait_recovery
    history = [{"ts": t, "commits": 250, "retries": 5 if t < 1000 else 0, "errors": 0} for t in range(950, 1010)]
    workload = SimpleNamespace(check_invariant=lambda: None,
                               status=lambda: {"recovery_s": 0.0, "money_lost": 0, "failure_at": 1000.4,
                                               "history": history})
    result = _wait_recovery(ctx_for(STRETCH, workload=workload))
    assert result["Recovery time"] == "0 s" and result["Transfers retried"] == 0, result


def test_what_to_look_at_is_filled_in_like_the_caption():
    # the stage shows each step's observe line under the caption: no raw {placeholders} on the big screen
    prov = SimpleNamespace(topology=AWS9)
    ctx = Context(provisioner=prov, chaos=FakeChaos(), elastic=FakeElastic(prov), workload=FakeWorkload(), operations=Operations(),
                  events=Events(), connect=RecordingDB().connect, lb_host="haproxy", catalog=CAT, sleep=lambda s: None)
    runner = Runner(ALL, ctx, run_in_background=lambda f: f())
    runner.start("survive-region", AWS9, "realistic")
    assert runner.status()["steps"][1]["observe"] == "Every node in us-east-1 is gone. Leaseholders move to surviving regions."
    runner.stop()


def test_domiciling_checks_each_customer_once_payments_have_recovered(monkeypatch):
    """Losing a region stalls the whole cluster for a few seconds while the leaders it held are re-elected. A customer
    in a healthy region must not be reported as waiting for their region because the check raced that."""
    from controlplane.scenarios import library as L
    status = {"recovery_s": None, "money_lost": 0, "history": [], "failure_at": 1.0}
    asked = []

    def workload_status():
        asked.append(1)
        if len(asked) >= 3:
            status["recovery_s"] = 6.0   # payments are back after a few polls
        return status

    written = []   # (region, had payments recovered when this customer was written?)
    monkeypatch.setattr(L, "_write", lambda ctx, region, name, seconds=6:
                        written.append((region, status["recovery_s"] is not None)) or "committed")
    ctx = SimpleNamespace(chaos=SimpleNamespace(act=lambda *a, **k: None), sleep=lambda s: None, clock=lambda: 0.0,
                          workload=SimpleNamespace(mark_failure=lambda: None, status=workload_status, check_invariant=lambda: None))
    regions = ["us-east-1", "us-west-2", "eu-west-1"]
    out = L._domicile_region_down(ctx, {"abroad": "eu-west-1", "regions": regions, "people": {r: [("P", "c")] for r in regions}})
    assert [r for r, _ in written] == regions
    assert all(recovered for _, recovered in written), written
    assert out["Payments"] == "recovered in 6 s, $0 lost"


def test_regional_by_row_promises_local_reads_not_local_write_speed():
    """Measured on a REGION-survival cluster: a row read at home takes 1-3 ms, but a write at home takes 60-90 ms,
    because a second region confirms it. The summary on the stage must not promise 1-2 ms writes."""
    summary = next(s for s in ALL if s.id == "regional-by-row").summary
    assert "writes in ~1-2 ms" not in summary
    assert "reads in ~1-2 ms" in summary and "home region" in summary


def test_data_domiciling_gives_every_region_its_own_customers():
    """The default AWS regions put two on one continent (N. Virginia and Oregon). They must not share the same people:
    results are shown per customer, so one region's Maya would replace the other's."""
    from controlplane.scenarios import library as L
    state = {}
    L._domicile_prepare(ctx_for(AWS9), state)
    assert state["regions"] == ["us-east-1", "us-west-2", "eu-west-1"]
    everyone = [person for region in state["regions"] for person in state["people"][region]]
    assert len(set(everyone)) == len(everyone) == 9, everyone
    assert state["people"]["us-east-1"][0] == ("Maya", "New York")       # the first region on a continent keeps its names
    assert state["people"]["eu-west-1"][0] == ("Aoife", "Dublin")
    assert {city for _, city in state["people"]["us-west-2"]} == {"Oregon"}   # the second one is named after its place
    assert state["abroad"] == "eu-west-1" and state["abroad_customer"] == "Aoife"


def test_data_domiciling_needs_three_regions_two_of_them_with_three_nodes():
    pass
    runner = Runner(ALL, ctx_for(AWS), run_in_background=lambda f: f())
    reason = "needs 3+ regions, two of them with 3 nodes each: build the residency preset"
    assert {s["id"]: s for s in runner.catalog(AWS, "realistic")}["data-domicile"]["unmet"] == [reason]
    # two regions can't work: one of them always holds the majority of the cluster's own data
    two = parse_preset("two", {"regions": [{"name": "us-east-1", "provider": "aws", "nodes": 3, "primary": True},
                                           {"name": "eu-west-1", "provider": "aws", "nodes": 3}]}, CAT, **KW)
    assert reason in {s["id"]: s for s in runner.catalog(two, "realistic")}["data-domicile"]["unmet"]
    assert {s["id"]: s for s in runner.catalog(RESIDENCY, "realistic")}["data-domicile"]["unmet"] == []


def test_data_domiciling_keeps_each_customer_at_home_then_pauses_only_the_lost_region():
    db, chaos, prov = RecordingDB(), FakeChaos(), SimpleNamespace(topology=RESIDENCY)
    ctx = Context(provisioner=prov, chaos=chaos, elastic=FakeElastic(prov), workload=FakeWorkload(), operations=Operations(),
                  events=Events(), connect=db.connect, lb_host="haproxy", catalog=CAT, sleep=lambda s: None)
    runner = Runner(ALL, ctx, run_in_background=lambda f: f())
    runner.start("data-domicile", RESIDENCY, "realistic")
    for _ in [s for s in ALL if s.id == "data-domicile"][0].steps:
        runner.next()
    status = runner.status()
    assert all(s["status"] == "done" for s in status["steps"]), status["steps"]
    sql = db.sql
    # its own database across the three regions, ZONE survival, no secondary region (unsupported with domiciling)
    assert 'CREATE DATABASE residency PRIMARY REGION "us-east-1" REGIONS "eu-west-1", "ap-southeast-1" SURVIVE ZONE FAILURE' in sql
    assert not any("SECONDARY REGION" in q for q in sql if "residency" in q)
    # the table's region type must come from the residency database, not the session's default (chaos)
    create = next(i for i, q in enumerate(sql) if q.startswith("CREATE TABLE residency.customers"))
    assert "USE residency" in sql[:create]
    on = sql.index("SET enable_super_regions = 'on'")
    for name, region in (("americas", "us-east-1"), ("europe", "eu-west-1"), ("asia_pacific", "ap-southeast-1")):
        assert sql.index(f'ALTER DATABASE residency ADD SUPER REGION "{name}" VALUES "{region}"') > on
    # the farthest region from the primary loses a node, then the whole region
    crashed = [a for a in chaos.acts if a[0] == "crash"]
    assert len(crashed) == 1 and crashed[0][2].startswith("ap-southeast-1")
    assert ("recover", "node", crashed[0][2]) in chaos.acts
    assert chaos.acts.index(("outage", "region", "ap-southeast-1")) < chaos.acts.index(("recover", "region", "ap-southeast-1"))
    assert status["steps"][1]["result"]["Copies outside their home region"] == "0"
    runner.stop()
    assert "DROP DATABASE IF EXISTS residency CASCADE" in db.sql


def test_default_placement_is_three_copies_at_home_plus_one_read_only_copy_per_other_region():
    from controlplane.scenarios.library import _default_placed, _where
    regions = ["us-east-1", "eu-west-1", "ap-southeast-1"]
    assert _default_placed("us-east-1", ["us-east-1"] * 3 + ["eu-west-1", "ap-southeast-1"], regions)
    # which other region holds each read-only copy can take minutes to even out; the count is what the step shows
    assert _default_placed("us-east-1", ["us-east-1"] * 3 + ["ap-southeast-1"] * 2, regions)
    assert not _default_placed("us-east-1", ["us-east-1"] * 2 + ["eu-west-1", "ap-southeast-1"], regions)
    assert not _default_placed("us-east-1", ["us-east-1"] * 3 + ["eu-west-1"], regions)
    assert _where("eu-west-1", ["eu-west-1"] * 3 + ["ap-southeast-1"] * 2) == "3 in eu-west-1, 2 outside"
    assert _where("eu-west-1", ["eu-west-1"] * 3) == "3 in eu-west-1, none outside"
