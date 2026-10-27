"""The showcase scenarios (spec §8): Tier 1 resilience and multi-region, Tier 2 transactions and operations."""
from __future__ import annotations

import math
import threading
import uuid

from ..bootstrap import _literal
from ..retry import backoff_delay
from . import Context, Scenario, ScenarioError, Step

CDC_PORT = 8443


def _uuid(*parts) -> str:
    """Deterministic UUID keys: random-looking (no hot spots) yet known in advance for lookups."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "crdb-chaos/" + "/".join(map(str, parts))))


# -- helpers ----------------------------------------------------------------------------------------------------

def _distance(a, b) -> float:
    if not a.location or not b.location:
        return 0.0
    (la1, lo1), (la2, lo2) = a.location, b.location
    p1, p2, dl = math.radians(la1), math.radians(la2), math.radians(lo2 - lo1)
    return math.degrees(math.acos(max(-1.0, min(1.0, math.sin(p1) * math.sin(p2) + math.cos(p1) * math.cos(p2) * math.cos(dl)))))


def _far(topo) -> str:
    others = [r for r in topo.regions if not r.primary]
    if not others:
        raise ScenarioError("needs a second region")
    return max(others, key=lambda r: (_distance(topo.primary, r), r.index)).name


def _base_state(ctx: Context, state: dict) -> None:
    topo = ctx.topo
    state["primary"] = topo.primary.name
    state["display_name"] = topo.display_name
    if len(topo.regions) > 1:
        state["far"] = _far(topo)


def _ensure_payments(ctx: Context) -> None:
    if not ctx.workload.running:
        ctx.workload.start()
        ctx.sleep(12)  # build a 10 s baseline for the recovery-time measurement


def _wait_recovery(ctx: Context, timeout: float = 90) -> dict:
    deadline = ctx.clock() + timeout
    status = ctx.workload.status()
    while ctx.clock() < deadline:
        status = ctx.workload.status()
        if status["recovery_s"] is not None:
            break
        ctx.sleep(1)
    try:
        ctx.workload.check_invariant()
        status = ctx.workload.status()
    except Exception:
        pass
    lost = status["money_lost"]
    return {"Recovery time": "still recovering" if status["recovery_s"] is None else f"{status['recovery_s']:.0f} s",
            "Money lost": "–" if lost is None else f"${lost:,}",
            "Transfers retried": sum(h["retries"] for h in status["history"][-60:])}


def _commits_last(ctx: Context, seconds: int) -> int:
    return sum(h["commits"] for h in ctx.workload.status()["history"][-seconds:])


def _set_goal(ctx: Context, goal: str) -> None:
    if ctx.topo.survival_goal != goal:
        ctx.elastic.set_survival(goal)
        ctx.sleep(0.5)
        ctx.wait_operation()


def _wait_replicated(ctx: Context, timeout: float = 300) -> float:
    started = ctx.clock()
    while ctx.clock() - started < timeout:
        row = ctx.query("SELECT COALESCE(sum((metrics->>'ranges.underreplicated')::FLOAT), 0) FROM crdb_internal.kv_store_status",
                        one=True)
        if not row[0]:
            return ctx.clock() - started
        ctx.sleep(3)
    raise ScenarioError("replicas are still moving; check the DB Console")


def _node_regions(ctx: Context) -> dict[int, str]:
    region_of = {n.name: n.region for n in ctx.topo.nodes}
    return {int(i): region_of.get(str(a).rsplit(":", 1)[0], "?")
            for i, a in ctx.query("SELECT node_id, address FROM crdb_internal.gossip_nodes")}


def _wait_voters(ctx: Context, region: str, timeout: float = 300) -> float:
    """Wait for ZONE survival placement of the accounts table: every range has all its voting replicas in one
    region. The table is REGIONAL BY ROW, so each partition keeps its voters in its own home region and the
    table's own ranges in `region` (the primary); no single region holds them all."""
    started = ctx.clock()
    while ctx.clock() - started < timeout:
        where = _node_regions(ctx)
        ranges = ctx.query("SELECT voting_replicas FROM [SHOW RANGES FROM TABLE accounts WITH DETAILS]")
        if ranges and all(len({where.get(int(v)) for v in row[0]}) == 1 for row in ranges):
            return ctx.clock() - started
        ctx.sleep(3)
    raise ScenarioError("voting replicas are still moving into their home regions; try again in a minute")


def _outage(region_key: str):
    def act(ctx: Context, state: dict) -> dict:
        result = ctx.chaos.act("outage", "region", state[region_key], confirm=True)
        ctx.workload.mark_failure()
        return {"Nodes down": len(result["targets"])}
    return act


def _recover(region_key: str | None = None, settle: float = 0):
    def act(ctx: Context, state: dict) -> dict:
        if region_key:
            ctx.chaos.act("recover", "region", state[region_key])
        else:
            ctx.chaos.recover_all()
        ctx.sleep(settle)
        return {"Recovered": state[region_key] if region_key else "everything"}
    return act


def _settle(ctx: Context, sql_for, regions, budget: float = 90, local_ms: float = 15) -> None:
    """Wait until each region's gateway answers its own query locally: new tables take a while to place replicas."""
    deadline = ctx.clock() + budget
    while ctx.clock() < deadline:
        try:
            if all(ctx.timed(sql_for(g), ctx.gateway(g), repeat=1) < local_ms for g in regions):
                return
        except Exception:
            pass
        ctx.sleep(3)


def _pin_leases(ctx: Context, table: str, rows: dict[str, str]) -> None:
    """Move each partition's lease to its home region now instead of waiting minutes for the allocator.
    rows: region -> SQL tuple of a row homed there, e.g. "('us-east-1', 101)"."""
    for region, row in rows.items():
        try:
            info = ctx.query(f"SELECT range_id, lease_holder, replicas, replica_localities "
                             f"FROM [SHOW RANGE FROM TABLE {table} FOR ROW {row}]", one=True)
        except Exception:
            continue
        if not info:
            continue
        range_id, holder, replicas, localities = info
        home = [int(r) for r, loc in zip(replicas, localities) if f"region={region}," in f"{loc},"]
        if home and int(holder) not in home:
            try:
                ctx.run(f"ALTER RANGE {int(range_id)} RELOCATE LEASE TO {home[0]}")
            except Exception:
                pass  # the allocator gets there on its own; the settle wait covers it


def _drop(*tables: str):
    def reset(ctx: Context, state: dict) -> None:
        ctx.run([f"DROP TABLE IF EXISTS {t}" for t in tables])
    return reset


def _recover_and_drop(*tables: str):
    def reset(ctx: Context, state: dict) -> None:
        ctx.chaos.recover_all()
        if tables:
            ctx.sleep(3)
            ctx.run([f"DROP TABLE IF EXISTS {t}" for t in tables])
    return reset


# -- 1. survive a region outage ---------------------------------------------------------------------------------

def _survive_prepare(ctx: Context, state: dict) -> None:
    _base_state(ctx, state)
    _set_goal(ctx, "region")


def _survive_measure(ctx: Context, state: dict) -> dict:
    return _wait_recovery(ctx)


def _survive_start(ctx: Context, state: dict) -> dict:
    _ensure_payments(ctx)
    return {"Payments": f"{ctx.workload.rate} transfers/s target"}


SURVIVE = Scenario(
    id="survive-region", title="Survive a region outage", tier=1, min_regions=3, min_nodes=5,
    summary="Lose a whole region under load: no downtime, no data loss, automatic recovery.",
    prepare=_survive_prepare, reset=lambda ctx, st: ctx.chaos.recover_all(),
    steps=(
        Step("Start payments", "{display_name} is moving money in every region", _survive_start,
             "Transactions per second, by region"),
        Step("Take down {primary}", "{primary} just went dark", _outage("primary"),
             "Every node in {primary} is gone. Leaseholders move to surviving regions."),
        Step("Measure the impact", "Payments keep committing. Not one dollar lost.", _survive_measure,
             "Recovery time, money lost, unavailable ranges"),
        Step("Bring {primary} back", "{primary} rejoins and catches up on its own", _recover("primary"),
             "Nodes come back live; replicas and leases return"),
    ),
)


# -- 1b. survive a zone outage (the single-region headline) ------------------------------------------------------

def _zone_prepare(ctx: Context, state: dict) -> None:
    _base_state(ctx, state)
    zones = [z for z in ctx.topo.primary.zones if any(n.zone == z for n in ctx.topo.nodes)]
    state["zone"] = zones[0]


def _zone_outage(ctx: Context, state: dict) -> dict:
    result = ctx.chaos.act("outage", "zone", state["zone"], confirm=True)
    ctx.workload.mark_failure()
    return {"Nodes down": len(result["targets"]), "Zone": state["zone"]}


SURVIVE_ZONE = Scenario(
    id="survive-zone", title="Survive a zone outage", tier=1, min_regions=1, min_nodes=3, min_primary_nodes=3,
    summary="Lose a whole availability zone under load: no downtime, no data loss, automatic recovery.",
    prepare=_zone_prepare, reset=lambda ctx, st: ctx.chaos.recover_all(),
    steps=(
        Step("Start payments", "{display_name} is moving money", _survive_start, "Transactions per second"),
        Step("Take down {zone}", "Availability zone {zone} just went dark", _zone_outage,
             "Every node in {zone} is gone. The other zones still hold a majority of every range."),
        Step("Measure the impact", "Payments keep committing. Not one dollar lost.", _survive_measure,
             "Recovery time, money lost, unavailable ranges"),
        Step("Bring {zone} back", "{zone} rejoins and catches up on its own",
             lambda ctx, st: (ctx.chaos.act("recover", "zone", st["zone"]), {"Recovered": st["zone"]})[1]),
    ),
)


# -- 2. ZONE vs REGION survival ---------------------------------------------------------------------------------

def _zone_step(ctx: Context, state: dict) -> dict:
    _set_goal(ctx, "zone")
    _ensure_payments(ctx)
    took = _wait_voters(ctx, state["primary"])
    return {"Survival goal": "ZONE", "Voting replicas": "3 per partition, all in the partition's home region",
            "Placed in": f"{took:.0f} s"}


def _stall(ctx: Context, state: dict) -> dict:
    """Under ZONE survival the accounts homed in the primary lose their quorum with it; the other regions'
    partitions keep their voters at home and keep paying."""
    primary = state["primary"]
    ctx.chaos.act("outage", "region", primary, confirm=True)
    ctx.workload.mark_failure()
    ctx.sleep(15)
    homes = ctx.workload.status().get("by_home") or {}
    mine = (homes.get(primary) or {}).get("tps", 0)
    others = sum((v or {}).get("tps", 0) for r, v in homes.items() if r != primary)
    return {"Transfers in the last 10 s": _commits_last(ctx, 10),
            f"Payments homed in {primary}": "stalled until the region returns" if mine == 0 else f"slowed: {mine}/s",
            "Other regions": f"keep paying: {others}/s" if others else "no payments seen yet"}


def _region_step(ctx: Context, state: dict) -> dict:
    _set_goal(ctx, "region")
    took = _wait_replicated(ctx)
    return {"Survival goal": "REGION", "Rebalanced in": f"{took:.0f} s"}


def _survive_again(ctx: Context, state: dict) -> dict:
    ctx.chaos.act("outage", "region", state["primary"], confirm=True)
    ctx.workload.mark_failure()
    ctx.sleep(15)
    out = _wait_recovery(ctx, timeout=60)
    out["Transfers in the last 10 s"] = _commits_last(ctx, 10)
    return out


ZONE_VS_REGION = Scenario(
    id="zone-vs-region", title="ZONE vs REGION survival", tier=1, min_regions=3, min_nodes=5, min_primary_nodes=3,
    summary="The same region outage, before and after one ALTER DATABASE statement.",
    prepare=lambda ctx, st: _base_state(ctx, st), reset=lambda ctx, st: ctx.chaos.recover_all(),
    steps=(
        Step("Switch to ZONE survival", "Survival goal: ZONE. Each partition's voters live in its home region.", _zone_step),
        Step("Take down {primary}", "Under ZONE survival, losing {primary} stalls the payments homed there", _stall,
             "Unavailable ranges climb; {primary}'s transfers stop while the other regions keep paying"),
        Step("Recover {primary}", "{primary} is back; writes resume", _recover("primary", settle=10)),
        Step("Switch to REGION survival", "ALTER DATABASE chaos SURVIVE REGION FAILURE", _region_step,
             "Replicas spread across regions while traffic keeps flowing"),
        Step("Take down {primary} again", "Same failure. This time every region keeps paying.", _survive_again),
        Step("Recover", "Back to full strength", _recover("primary")),
    ),
)


# -- 3. elastic scale -------------------------------------------------------------------------------------------

def _elastic_prepare(ctx: Context, state: dict) -> None:
    _base_state(ctx, state)
    topo = ctx.topo
    provider = topo.primary.provider
    have = {r.name for r in topo.regions}
    choices = [r.name for r in ctx.catalog.regions_for(provider) if r.name not in have] if ctx.catalog else []
    state["provider"] = provider
    state["new_region"] = choices[0] if choices else "new-region-1"


def _add_node(ctx: Context, state: dict) -> dict:
    before = {n.name for n in ctx.topo.nodes}
    ctx.elastic.add_node(state["primary"])
    ctx.sleep(0.5)
    ctx.wait_operation()
    added = [n.name for n in ctx.topo.nodes if n.name not in before]
    state["added_node"] = added[0] if added else ""
    return {"New node": state["added_node"], "Watch": "replicas and leases flow onto it"}


def _add_region(ctx: Context, state: dict) -> dict:
    ctx.elastic.add_region(state["new_region"], state["provider"], 3)
    ctx.sleep(0.5)
    ctx.wait_operation()
    return {"New region": state["new_region"], "Nodes": 3}


def _decommission(ctx: Context, state: dict) -> dict:
    started = ctx.clock()
    ctx.elastic.decommission(state["added_node"])
    ctx.sleep(0.5)
    ctx.wait_operation()
    return {"Drained": state["added_node"], "Took": f"{ctx.clock() - started:.0f} s"}


def _remove_region(ctx: Context, state: dict) -> dict:
    ctx.elastic.remove_region(state["new_region"])
    ctx.sleep(0.5)
    ctx.wait_operation()
    return {"Removed": state["new_region"]}


ELASTIC = Scenario(
    id="elastic-scale", title="Elastic scale", tier=1, min_regions=1, min_nodes=3,
    summary="Add a node and a whole region live, then drain them away. No maintenance window.",
    prepare=_elastic_prepare,
    steps=(
        Step("Add a node in {primary}", "Scaling out: one more node in {primary}", _add_node),
        Step("Add region {new_region}", "A new region joins the cluster and the databases", _add_region),
        Step("Decommission the new node", "Scaling in: replicas drain off before the node leaves", _decommission),
        Step("Remove {new_region}", "Region retired cleanly", _remove_region),
    ),
)


# -- 4. REGIONAL BY ROW -----------------------------------------------------------------------------------------

def _rbr_prepare(ctx: Context, state: dict) -> None:
    _base_state(ctx, state)
    regions = [r.name for r in ctx.topo.regions]
    state["regions"] = regions
    rows = ", ".join(f"({_literal(r)}, '{_uuid('rbr', r, k)}', 'customer {k} in {r}')" for r in regions for k in range(1, 6))
    ctx.run(["DROP TABLE IF EXISTS rbr_customers",
             "CREATE TABLE rbr_customers (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name STRING) LOCALITY REGIONAL BY ROW",
             f"INSERT INTO rbr_customers (crdb_region, id, name) VALUES {rows}"])
    _pin_leases(ctx, "rbr_customers", {r: f"({_literal(r)}, '{_uuid('rbr', r, 1)}')" for r in regions})
    _settle(ctx, lambda g: f"SELECT name FROM rbr_customers WHERE crdb_region = {_literal(g)} AND id = '{_uuid('rbr', g, 1)}'",
            regions)


def _rbr_matrix(ctx: Context, state: dict, write: bool) -> dict:
    matrix = {}
    for g in state["regions"]:
        host = ctx.gateway(g)
        matrix[g] = {}
        for r in state["regions"]:
            row_id = _uuid("rbr", r, 1)
            sql = (f"UPSERT INTO rbr_customers (crdb_region, id, name) VALUES ({_literal(r)}, '{row_id}', 'updated')" if write else
                   f"SELECT name FROM rbr_customers WHERE crdb_region = {_literal(r)} AND id = '{row_id}'")
            matrix[g][r] = ctx.timed(sql, host, repeat=3)
    return {"matrix": matrix, "rows": "row's home region", "cols": "gateway region"}


RBR = Scenario(
    id="regional-by-row", title="REGIONAL BY ROW", tier=1, min_regions=2, needs_latency=True,
    summary="Each customer's row lives in their region: local reads and writes in ~1-2 ms everywhere.",
    prepare=_rbr_prepare, reset=_drop("rbr_customers"),
    steps=(
        Step("Read from every region", "Reads: local rows are fast, remote rows pay the round trip",
             lambda ctx, st: _rbr_matrix(ctx, st, write=False), "The diagonal is local"),
        Step("Write from every region", "Writes: committed in the row's home region",
             lambda ctx, st: _rbr_matrix(ctx, st, write=True)),
    ),
)


PRICE_ID = _uuid("price", 42)


# -- 5. follower reads ------------------------------------------------------------------------------------------

def _fr_prepare(ctx: Context, state: dict) -> None:
    _base_state(ctx, state)
    ctx.run(["DROP TABLE IF EXISTS fr_prices",
             "CREATE TABLE fr_prices (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), price DECIMAL NOT NULL)",
             "INSERT INTO fr_prices (id, price) VALUES " + ", ".join(f"('{_uuid('price', i)}', {i * 1.25})" for i in range(1, 101))])
    _settle(ctx, lambda g: f"SELECT price FROM fr_prices AS OF SYSTEM TIME follower_read_timestamp() WHERE id = '{PRICE_ID}'",
            [state["far"]])


def _fr_strong(ctx: Context, state: dict) -> dict:
    ms = ctx.timed(f"SELECT price FROM fr_prices WHERE id = '{PRICE_ID}'", ctx.gateway(state["far"]))
    state["strong_ms"] = ms
    return {f"Read from {state['far']}": f"{ms} ms", "Served by": f"leaseholder in {state['primary']}"}


def _fr_follower(ctx: Context, state: dict) -> dict:
    ms = ctx.timed(f"SELECT price FROM fr_prices AS OF SYSTEM TIME follower_read_timestamp() WHERE id = '{PRICE_ID}'",
                   ctx.gateway(state["far"]))
    out = {f"Follower read from {state['far']}": f"{ms} ms", "Served by": f"local replica in {state['far']}"}
    if state.get("strong_ms") and ms:
        out["Faster by"] = f"{state['strong_ms'] / max(ms, 0.1):.0f}×"
    return out


FOLLOWER = Scenario(
    id="follower-reads", title="Follower reads", tier=1, min_regions=2, needs_latency=True,
    summary="Slightly stale reads served by the nearest replica instead of a distant leaseholder.",
    prepare=_fr_prepare, reset=_drop("fr_prices"),
    steps=(
        Step("Normal read from {far}", "From {far}, a normal read travels to {primary}", _fr_strong),
        Step("Follower read from {far}", "AS OF SYSTEM TIME follower_read_timestamp(): answered locally", _fr_follower),
    ),
)


# -- 6. GLOBAL tables -------------------------------------------------------------------------------------------

def _global_prepare(ctx: Context, state: dict) -> None:
    _base_state(ctx, state)
    state["regions"] = [r.name for r in ctx.topo.regions]
    ctx.run(["DROP TABLE IF EXISTS fx_rates",
             "CREATE TABLE fx_rates (ccy STRING PRIMARY KEY, rate DECIMAL NOT NULL) LOCALITY GLOBAL",
             "INSERT INTO fx_rates VALUES ('EUR', 0.92), ('GBP', 0.79), ('JPY', 149.5), ('INR', 83.2)"])
    _settle(ctx, lambda g: "SELECT rate FROM fx_rates WHERE ccy = 'EUR'", state["regions"])


def _global_reads(ctx: Context, state: dict) -> dict:
    return {f"Read in {g}": f"{ctx.timed('SELECT rate FROM fx_rates WHERE ccy = ' + _literal('EUR'), ctx.gateway(g))} ms"
            for g in state["regions"]}


def _global_write(ctx: Context, state: dict) -> dict:
    ms = ctx.timed("UPSERT INTO fx_rates VALUES ('EUR', 0.93)", ctx.gateway(state["primary"]), repeat=3)
    return {"Write": f"{ms} ms", "Why": "commit waits out clock uncertainty so every region can read locally"}


GLOBAL = Scenario(
    id="global-tables", title="GLOBAL tables", tier=1, min_regions=2, needs_latency=True,
    summary="Read-mostly reference data: fast, consistent reads in every region; writes cost more.",
    prepare=_global_prepare, reset=_drop("fx_rates"),
    steps=(
        Step("Read from every region", "GLOBAL table: every region reads locally, strongly consistent", _global_reads),
        Step("Write once", "Writes pay once so reads never have to", _global_write),
    ),
)


# -- 7. isolation levels: write skew ---------------------------------------------------------------------------

def _skew_prepare(ctx: Context, state: dict) -> None:
    try:
        ctx.run("SET CLUSTER SETTING sql.txn.read_committed_isolation.enabled = true")
    except Exception:
        pass  # already on, or not settable on this version
    ctx.run(["DROP TABLE IF EXISTS joint_account",
             "CREATE TABLE joint_account (account STRING PRIMARY KEY, balance INT NOT NULL)"])
    _skew_reset_rows(ctx)


def _skew_reset_rows(ctx: Context) -> None:
    ctx.run("UPSERT INTO joint_account VALUES ('checking', 50), ('savings', 50)")


def _withdraw(ctx: Context, isolation: str, account: str, barrier: threading.Barrier, out: dict) -> None:
    conn = ctx.connect()
    attempts = 0
    try:
        with conn.cursor() as cur:
            while attempts < 12:
                attempts += 1
                try:
                    cur.execute(f"BEGIN TRANSACTION ISOLATION LEVEL {isolation}")
                    cur.execute("SELECT sum(balance) FROM joint_account")
                    total = cur.fetchone()[0]
                    if attempts == 1:
                        barrier.wait(timeout=10)  # both withdrawals have now seen the same total
                    if total - 100 < 0:
                        cur.execute("ROLLBACK")
                        out[account] = "declined"
                        return
                    cur.execute(f"UPDATE joint_account SET balance = balance - 100 WHERE account = {_literal(account)}")
                    if attempts == 1:
                        try:
                            barrier.wait(timeout=10)
                        except threading.BrokenBarrierError:
                            pass
                    cur.execute("COMMIT")
                    out[account] = "committed" if attempts == 1 else f"committed after {attempts - 1} retry"
                    return
                except threading.BrokenBarrierError:
                    cur.execute("ROLLBACK")
                    raise
                except Exception as e:
                    try:
                        cur.execute("ROLLBACK")
                    except Exception:
                        pass
                    if getattr(e, "sqlstate", None) == "40001":
                        out.setdefault("retries", 0)
                        out["retries"] += 1
                        # back off like a real client: a conflict with a just-committed write (the clock
                        # uncertainty window) clears within a few hundred milliseconds
                        ctx.sleep(backoff_delay(attempts - 1, initial_backoff=0.05, max_backoff=1.0))
                        continue
                    out[account] = f"error: {e}"
                    return
            out[account] = f"gave up after {attempts} retries"
    finally:
        conn.close()


def _skew(isolation: str):
    def act(ctx: Context, state: dict) -> dict:
        _skew_reset_rows(ctx)
        out: dict = {}
        barrier = threading.Barrier(2)
        threads = [threading.Thread(target=_withdraw, args=(ctx, isolation, acct, barrier, out))
                   for acct in ("checking", "savings")]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        total = ctx.query("SELECT sum(balance) FROM joint_account", one=True)[0]
        return {"From checking": out.get("checking", "?"), "From savings": out.get("savings", "?"),
                "Retries": out.get("retries", 0), "Checking + savings": f"${total}",
                "Rule (≥ $0)": "kept ✓" if total >= 0 else "BROKEN ✗"}
    return act


SKEW = Scenario(
    id="write-skew", title="Isolation levels: real write skew", tier=2, min_regions=1,
    summary="Two concurrent withdrawals against a joint account rule. READ COMMITTED breaks it; SERIALIZABLE keeps it.",
    prepare=_skew_prepare, reset=_drop("joint_account"),
    steps=(
        Step("Race under READ COMMITTED", "Rule: checking + savings ≥ $0. Two $100 withdrawals, same instant.",
             _skew("READ COMMITTED"), "Both see $100 available; both commit"),
        Step("Race under SERIALIZABLE", "Same race, SERIALIZABLE: one withdrawal retries and is declined",
             _skew("SERIALIZABLE")),
    ),
)


# -- 8. online schema change -----------------------------------------------------------------------------------

def _schema(sql: str, label: str):
    def act(ctx: Context, state: dict) -> dict:
        _ensure_payments(ctx)
        before = ctx.workload.status()["failed_total"]
        started = ctx.clock()
        ctx.run(sql)
        took = ctx.clock() - started
        after = ctx.workload.status()["failed_total"]
        return {label: f"done in {took:.1f} s", "Failed transfers meanwhile": after - before,
                "Transfers meanwhile": _commits_last(ctx, max(1, int(took) + 1))}
    return act


def _schema_reset(ctx: Context, state: dict) -> None:
    ctx.run(["DROP INDEX IF EXISTS accounts@accounts_balance_idx", "ALTER TABLE accounts DROP COLUMN IF EXISTS memo"])


SCHEMA = Scenario(
    id="online-schema-change", title="Online schema change under load", tier=2, min_regions=1,
    summary="Add a column and build an index on a live table while payments keep flowing.",
    reset=_schema_reset,
    steps=(
        Step("Start payments", "Payments running against the accounts table", lambda ctx, st: _survive_start(ctx, st)),
        Step("ADD COLUMN", "ALTER TABLE accounts ADD COLUMN memo …, live",
             _schema("ALTER TABLE accounts ADD COLUMN IF NOT EXISTS memo STRING NOT NULL DEFAULT 'transfer'", "ADD COLUMN")),
        Step("CREATE INDEX", "CREATE INDEX on balance, live",
             _schema("CREATE INDEX IF NOT EXISTS accounts_balance_idx ON accounts (balance)", "CREATE INDEX")),
    ),
)


# -- 9. changefeeds ---------------------------------------------------------------------------------------------

def _cdc_prepare(ctx: Context, state: dict) -> None:
    _base_state(ctx, state)
    if ctx.cdc is None:
        raise ScenarioError("the changefeed sink is not running")
    ctx.cdc.reset()


def _cdc_create(ctx: Context, state: dict) -> dict:
    _ensure_payments(ctx)
    sink = f"webhook-https://{state.get('sink_host', 'crdb-chaos-control-plane')}:{CDC_PORT}/cdc?insecure_tls_skip_verify=true"
    row = ctx.query(f"CREATE CHANGEFEED FOR TABLE accounts INTO {_literal(sink)} "
                    "WITH updated, resolved = '2s', min_checkpoint_frequency = '2s'", one=True, idempotent=False)
    state["job"] = int(row[0])
    ctx.sleep(8)
    st = ctx.cdc.status()
    return {"Changefeed job": state["job"], "Events received": st["events"], "Events/s": st["events_s"]}


def _cdc_crash(ctx: Context, state: dict) -> dict:
    row = ctx.query(f"SELECT coordinator_id FROM crdb_internal.jobs WHERE job_id = {int(state['job'])}", one=True)
    coord = row[0] if row else None
    rows = ctx.query("SELECT node_id, address FROM crdb_internal.gossip_nodes")
    by_id = {int(i): str(a).rsplit(":", 1)[0] for i, a in rows}
    victim = by_id.get(int(coord)) if coord is not None else None
    if victim is None:
        victim = next(n.name for n in ctx.topo.nodes if n.region == state["primary"])
    state["victim"] = victim
    state["crashed_at"] = ctx.clock()
    ctx.chaos.act("crash", "node", victim, confirm=True)
    return {"Crashed": victim, "It was running": f"changefeed {state['job']}"}


def _cdc_resume(ctx: Context, state: dict) -> dict:
    deadline = ctx.clock() + 120
    while ctx.clock() < deadline:
        st = ctx.cdc.status()
        if st["resolved_lag_s"] is not None and st["resolved_lag_s"] < 10 and ctx.cdc.resolved_at and \
                ctx.cdc.resolved_at > state["crashed_at"] + 2:
            break
        ctx.sleep(2)
    st = ctx.cdc.status()
    row = ctx.query(f"SELECT coordinator_id FROM crdb_internal.jobs WHERE job_id = {int(state['job'])}", one=True)
    return {"Resumed on node": row[0] if row else "?", "Longest pause in resolved timestamps": f"{st['max_gap_s']} s",
            "Events received": st["events"], "Gaps": "none: resumes from its last checkpoint"}


def _cdc_reset(ctx: Context, state: dict) -> None:
    ctx.chaos.recover_all()
    if state.get("job"):
        try:
            ctx.run(f"CANCEL JOB {int(state['job'])}")
        except Exception:
            pass


CDC = Scenario(
    id="changefeeds", title="Changefeeds that survive failure", tier=2, min_regions=1, min_nodes=3,
    summary="Stream every committed transfer to a webhook; crash the node running the feed and watch it resume.",
    prepare=_cdc_prepare, reset=_cdc_reset,
    steps=(
        Step("Create the changefeed", "Every committed transfer streams out as an event", _cdc_create),
        Step("Crash the feed's node", "The node coordinating the changefeed just crashed", _cdc_crash),
        Step("Watch it resume", "Another node picks up the job from its last checkpoint", _cdc_resume),
        Step("Recover the node", "Back to full strength", lambda ctx, st: (ctx.chaos.act("recover", "node", st["victim"]), {"Recovered": st["victim"]})[1]),
    ),
)


# -- 10. split-brain test --------------------------------------------------------------------------------------

def _split_prepare(ctx: Context, state: dict) -> None:
    _base_state(ctx, state)
    ctx.run(["DROP TABLE IF EXISTS ledger",
             "CREATE TABLE ledger (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), side STRING NOT NULL, at TIMESTAMPTZ DEFAULT now())"])


def _split_isolate(ctx: Context, state: dict) -> dict:
    ctx.chaos.act("isolate", "region", state["far"], confirm=True)
    return {"Isolated": state["far"], "Clients": f"can still reach {state['far']}"}


def _try_insert(ctx: Context, host: str, side: str) -> str:
    try:
        conn = ctx.connect(host, statement_timeout="4s")
    except Exception as e:
        return f"unreachable ({e.__class__.__name__})"
    try:
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO ledger (side) VALUES ({_literal(side)})")
        return "committed"
    except Exception as e:
        code = getattr(e, "sqlstate", "")
        return "refused (timed out waiting for a quorum)" if code == "57014" else f"refused ({code or e.__class__.__name__})"
    finally:
        conn.close()


def _split_write(ctx: Context, state: dict) -> dict:
    return {f"Write via {state['far']}": _try_insert(ctx, ctx.gateway(state["far"], allow_isolated=True), state["far"]),
            f"Write via {state['primary']}": _try_insert(ctx, ctx.gateway(state["primary"]), state["primary"])}


def _split_heal(ctx: Context, state: dict) -> dict:
    ctx.chaos.act("recover", "region", state["far"])
    ctx.sleep(6)
    counts = {g: ctx.query("SELECT count(*), COALESCE(string_agg(side, ',' ORDER BY side), '') FROM ledger",
                           ctx.gateway(g), one=True) for g in (state["far"], state["primary"])}
    same = len({tuple(v) for v in counts.values()}) == 1
    out = {f"Rows seen from {g}": v[0] for g, v in counts.items()}
    out["Diverged"] = "no ✓" if same else "yes ✗"
    return out


SPLIT = Scenario(
    id="split-brain", title="Split-brain test", tier=2, min_regions=3, min_nodes=5,
    summary="Cut a region off from its peers. It refuses to commit alone; after healing, nothing has diverged.",
    prepare=_split_prepare, reset=_recover_and_drop("ledger"),
    steps=(
        Step("Isolate {far}", "{far} can't reach the other regions; its clients can still reach it", _split_isolate),
        Step("Write on both sides", "The isolated side cannot form a quorum, so it refuses to commit", _split_write),
        Step("Heal and compare", "Healed. Both sides agree on every row.", _split_heal),
    ),
)


# -- 11. time travel ------------------------------------------------------------------------------------------

def _tt_prepare(ctx: Context, state: dict) -> None:
    ctx.run(["DROP TABLE IF EXISTS orders",
             "CREATE TABLE orders (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), customer STRING NOT NULL, amount INT NOT NULL)",
             "INSERT INTO orders (customer, amount) SELECT 'customer ' || i::STRING, (i * 37) % 500 + 10 FROM generate_series(1, 500) AS i"])
    ctx.sleep(2)


def _tt_delete(ctx: Context, state: dict) -> dict:
    state["before"] = str(ctx.query("SELECT cluster_logical_timestamp()", one=True)[0])
    ctx.sleep(1)
    ctx.run("DELETE FROM orders WHERE true")
    left = ctx.query("SELECT count(*) FROM orders", one=True)[0]
    return {"Rows left": left, "Oops": "DELETE without a WHERE clause"}


def _tt_look(ctx: Context, state: dict) -> dict:
    n = ctx.query(f"SELECT count(*) FROM orders AS OF SYSTEM TIME {_literal(state['before'])}", one=True)[0]
    return {"Rows one moment ago": n, "Query": "AS OF SYSTEM TIME"}


def _tt_restore(ctx: Context, state: dict) -> dict:
    # AS OF SYSTEM TIME only applies to a whole read, so read the old rows, then write them back.
    rows = ctx.query(f"SELECT id, customer, amount FROM orders AS OF SYSTEM TIME {_literal(state['before'])}")
    for i in range(0, len(rows), 250):
        values = ", ".join(f"({_literal(str(r[0]))}, {_literal(str(r[1]))}, {int(r[2])})" for r in rows[i:i + 250])
        ctx.run(f"UPSERT INTO orders (id, customer, amount) VALUES {values}")
    return {"Rows restored": ctx.query("SELECT count(*) FROM orders", one=True)[0]}


TIME_TRAVEL = Scenario(
    id="time-travel", title="Time travel", tier=2, min_regions=1,
    summary="Recover from an accidental DELETE by reading the table as it was seconds ago.",
    prepare=_tt_prepare, reset=_drop("orders"),
    steps=(
        Step("Accidental DELETE", "Someone just deleted every order", _tt_delete),
        Step("Look back in time", "SELECT … AS OF SYSTEM TIME: the data is still there", _tt_look),
        Step("Restore", "Read the rows as they were, write them back: restored", _tt_restore),
    ),
)


ALL = [SURVIVE, SURVIVE_ZONE, ZONE_VS_REGION, ELASTIC, RBR, FOLLOWER, GLOBAL, SKEW, SCHEMA, CDC, SPLIT, TIME_TRAVEL]
