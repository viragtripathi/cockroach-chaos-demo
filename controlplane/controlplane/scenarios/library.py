"""The showcase scenarios (spec §8): Tier 1 resilience and multi-region, Tier 2 transactions and operations."""
from __future__ import annotations

import math
import threading
import uuid

from ..bootstrap import _ident, _literal
from ..explainers import PROVIDER_LABEL
from ..retry import abandon_after, backoff_delay
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
    failure_at = status.get("failure_at")  # retries from the failure on: not the ones a failure before it caused
    history = status["history"]
    since = [h for h in history if h.get("ts", 0) >= int(failure_at)] if failure_at else history[-60:]
    return {"Recovery time": "still recovering" if status["recovery_s"] is None else f"{status['recovery_s']:.0f} s",
            "Money lost": "–" if lost is None else f"${lost:,}",
            "Transfers retried": sum(h["retries"] for h in since)}


def _commits_last(ctx: Context, seconds: int) -> int:
    return sum(h["commits"] for h in ctx.workload.status()["history"][-seconds:])


def _set_goal(ctx: Context, goal: str) -> None:
    if ctx.topo.survival_goal != goal:
        ctx.elastic.set_survival(goal)
        ctx.sleep(0.5)
        ctx.wait_operation()


# CockroachDB writes each store's status (what kv_store_status reads) every 10 s: a restarted node gets this long for
# its own and its peers' next status to count it again before the counts are trusted
REJOIN_S = 15


def _not_whole(ctx: Context) -> str | None:
    """What still keeps the cluster from being whole, or None: every node live and up long enough for the store
    status to count it, and no range unavailable, missing a copy, or waiting for a snapshot to catch a copy up."""
    rows = ctx.query("SELECT address, is_live, extract(epoch FROM now() - started_at) FROM crdb_internal.gossip_nodes "
                     "ORDER BY node_id")
    # by name: a name that was decommissioned and added again keeps its newest node
    nodes = {str(a).rsplit(":", 1)[0]: (bool(live), float(up or 0)) for a, live, up in rows}
    names = [n.name for n in ctx.topo.nodes]
    down = [n for n in names if not nodes.get(n, (False, 0.0))[0]]
    if down:
        return f"{', '.join(down)} not live yet"
    rejoining = [n for n in names if nodes[n][1] < REJOIN_S]
    if rejoining:
        return f"{', '.join(rejoining)} just rejoined"
    unavailable, missing, waiting = ctx.query(
        "SELECT COALESCE(sum((metrics->>'ranges.unavailable')::FLOAT), 0), "
        "COALESCE(sum((metrics->>'ranges.underreplicated')::FLOAT), 0), "
        "COALESCE(sum((metrics->>'queue.raftsnapshot.pending')::FLOAT), 0) FROM crdb_internal.kv_store_status",
        one=True)
    if unavailable:
        return f"{unavailable:.0f} ranges unavailable"
    if missing:
        return f"{missing:.0f} ranges still missing a copy"
    if waiting:
        return f"{waiting:.0f} copies waiting for a snapshot"
    return None


def _wait_whole(ctx: Context, budget: float, calm: int = 2) -> float:
    """Seconds until the cluster is whole on `calm` checks in a row; ScenarioError once the budget is spent."""
    started, streak, behind = ctx.clock(), 0, "not checked yet"
    while True:
        try:
            behind = _not_whole(ctx)
        except Exception as e:  # a node that is still starting can refuse the query
            behind = str(e)
        streak = streak + 1 if behind is None else 0
        if streak >= calm:
            return ctx.clock() - started
        if ctx.clock() - started >= budget:
            raise ScenarioError(f"still catching up after {budget:.0f} s: {behind}")
        ctx.sleep(2)


def _wait_spread(ctx: Context, tables=("accounts", "transfers"), timeout: float = 300) -> float:
    """Wait for REGION survival placement of the Payments tables: every range keeps a majority of its voting replicas
    after losing any one region. Read from the ranges themselves: the under-replicated counter refreshes every 10 s,
    so right after the ALTER it still reads 0 while nothing has moved."""
    started, short = ctx.clock(), "not checked yet"
    while ctx.clock() - started < timeout:
        where = _node_regions(ctx)
        regions = set(where.values())
        short = None
        for table in tables:
            rows = ctx.query(f"SELECT range_id, voting_replicas FROM [SHOW RANGES FROM TABLE {table} WITH DETAILS]")
            for range_id, voters in rows:
                homes = [where.get(int(v)) for v in voters]
                lost = next((r for r in regions if 2 * sum(h != r for h in homes) <= len(homes)), None)
                if lost:
                    short = f"{table} range {range_id} can't lose {lost} yet"
                    break
            if short:
                break
        if short is None:
            return ctx.clock() - started
        ctx.sleep(3)
    raise ScenarioError(f"replicas are still spreading across regions ({short}); try again in a minute")


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


def _recover_node(key: str):
    def act(ctx: Context, state: dict) -> dict:
        ctx.chaos.act("recover", "node", state[key])
        return {"Recovered": state[key]}
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


# -- 1a. stretch cluster: two clouds serve, a third only votes ---------------------------------------------------

def _stretch_prepare(ctx: Context, state: dict) -> None:
    _base_state(ctx, state)
    topo = ctx.topo
    serving = [r for r in topo.regions if not r.tie_breaker and not r.primary]
    second = topo.secondary or serving[0]
    tie = topo.tie_breakers[0]
    cloud = lambda r: PROVIDER_LABEL.get(r.provider, r.provider)  # noqa: E731
    state.update(secondary=second.name, tie=tie.name, primary_cloud=cloud(topo.primary), secondary_cloud=cloud(second),
                 tie_cloud=cloud(tie))
    _set_goal(ctx, "region")


def _stretch_start(ctx: Context, state: dict) -> dict:
    _ensure_payments(ctx)
    workers = (ctx.workload.status().get("homing") or {}).get("workers") or {}
    try:  # display only: how many ranges the tie-breaker holds a copy of
        where = _node_regions(ctx)
        rows = ctx.query("SELECT node_id, (metrics->>'replicas')::INT FROM crdb_internal.kv_store_status")
        held = f"{sum(int(r[1] or 0) for r in rows if len(r) > 1 and where.get(int(r[0])) == state['tie'])} ranges"
    except Exception:
        held = "a copy of every range"
    return {"Payments homed in": ", ".join(r for r in workers if r != state["tie"]) or f"{state['primary']}, {state['secondary']}",
            f"Payments workers in {state['tie']}": workers.get(state["tie"], 0),
            f"Copies held by {state['tie']}": held}


def _stretch_back(ctx: Context, state: dict) -> dict:
    """Restarting the containers takes a second; the step ends when the cluster is whole again, because losing the
    tie-breaker next is harmless only once the primary cloud votes again with current copies."""
    ctx.chaos.act("recover", "region", state["primary"])
    took = _wait_whole(ctx, budget=180)
    return {"Recovered": state["primary"], "Caught up in": f"{took:.0f} s"}


def _stretch_tie_outage(ctx: Context, state: dict) -> dict:
    try:
        _wait_whole(ctx, budget=60, calm=1)
    except ScenarioError as e:
        raise ScenarioError(f"not yet, {e}: losing {state['tie']} now would leave too few copies for a majority") from e
    result = ctx.chaos.act("outage", "region", state["tie"], confirm=True)
    ctx.workload.mark_failure()
    return {"Nodes down": len(result["targets"]), **_wait_recovery(ctx, timeout=60)}


STRETCH = Scenario(
    id="stretch-cloud", title="Stretch cluster: lose a whole cloud", tier=1, min_regions=3, min_nodes=5,
    needs_tie_breaker=True,
    summary="Two clouds serve every payment and a third only votes: lose a whole cloud with no data lost, "
            "then lose the tie-breaker and nothing happens.",
    prepare=_stretch_prepare, reset=lambda ctx, st: ctx.chaos.recover_all(),
    steps=(
        Step("Start payments", "{primary_cloud} and {secondary_cloud} serve every payment. {tie} only votes.",
             _stretch_start, "Every range has a copy on all five nodes; leases live in {primary} and {secondary}"),
        Step("Take down {primary}", "All of {primary_cloud} just went dark", _outage("primary"),
             "Leases move to {secondary}; {tie} keeps the majority: 3 of 5 copies are left"),
        Step("Measure the impact", "Payments keep committing on {secondary_cloud}. Not one dollar lost.",
             _survive_measure, "Recovery time, money lost, unavailable ranges"),
        Step("Bring {primary} back", "{primary_cloud} rejoins and catches up on its own", _stretch_back,
             "Nodes come back live and catch up; leases drift back to {primary} over the next couple of minutes"),
        Step("Take down {tie}", "Now the tie-breaker goes dark. Nothing has to fail over.", _stretch_tie_outage,
             "It leads nothing with traffic: payments don't wait for it, so recovery time should stay at 0 s"),
        Step("Bring {tie} back", "{tie} rejoins and takes its copies back", _recover("tie"),
             "Five of five copies again"),
        Step("Two at once", "Losing {primary_cloud} and the tie-breaker together leaves {secondary_cloud} alone, short of "
             "a majority: writes would stop. The panel asks before it lets you do that.", None,
             "This is why the third site matters, even with one node"),
    ),
)


# -- 1c. data domiciling: each customer's data stays in their home region ----------------------------------------
# Super regions (CockroachDB's recommended way to domicile data), one per region, on a database of the scenario's own
# with ZONE survival: a domiciled row keeps all three copies in its home region, one per zone, so each such region
# needs three nodes. No secondary region: it is not supported together with domiciling. The cluster needs three
# regions or more: with two, one region always holds the majority of the cluster's own data, and losing it would stop
# every region, the opposite of the story.

CUSTOMERS = {"europe": [("Aoife", "Dublin"), ("Lukas", "Frankfurt"), ("Chiara", "Milan")],
             "americas": [("Maya", "New York"), ("Diego", "Miami"), ("Grace", "Toronto")],
             "asia_pacific": [("Mei", "Singapore"), ("Arjun", "Mumbai"), ("Hana", "Tokyo")]}


def _continent(region) -> str | None:
    if not region.location:
        return None
    lat, lon = region.location
    if -170 <= lon <= -30:
        return "americas"
    if -25 <= lon <= 45 and lat >= 34:
        return "europe"
    if lon > 60:
        return "asia_pacific"
    return None


def _domiciled(topo) -> list:
    """Regions that can keep all three copies of a row at home, the primary first."""
    big = [r for r in topo.regions if sum(n.region == r.name for n in topo.nodes) >= 3]
    return sorted(big, key=lambda r: not r.primary)


def _residency_check(topo) -> list[str]:
    if len(topo.regions) >= 3 and len(_domiciled(topo)) >= 2:
        return []
    return ["needs 3+ regions, two of them with 3 nodes each: build the residency preset"]


def _domicile_prepare(ctx: Context, state: dict) -> None:
    _base_state(ctx, state)
    regions = _domiciled(ctx.topo)
    home = regions[0]
    abroad = max(regions[1:], key=lambda r: (_distance(home, r), r.index))
    supers = [_continent(r) for r in regions]
    if None in supers or len(set(supers)) < len(supers):
        supers = [r.name for r in regions]  # unknown or shared continents: name each super region after its region
    # A continent's customers go to its first region; a second region there (N. Virginia and Oregon), or one on no
    # known continent, gets customers named after its own place. Results are shown per customer, so no two regions
    # may share them.
    people, taken = {}, set()
    for r in regions:
        continent = _continent(r)
        if continent in CUSTOMERS and continent not in taken:
            taken.add(continent)
            people[r.name] = CUSTOMERS[continent]
        else:
            people[r.name] = [("Alex", r.label), ("Sam", r.label), ("Kim", r.label)]
    labels = [r.label for r in regions]
    state.update(regions=[r.name for r in regions], supers=dict(zip([r.name for r in regions], supers)), people=people,
                 region_list=", ".join(labels[:-1]) + f" and {labels[-1]}", abroad=abroad.name,
                 abroad_label=abroad.label, abroad_customer=people[abroad.name][0][0],
                 victim=[n.name for n in ctx.topo.nodes if n.region == abroad.name][-1])


def _ranges(ctx: Context, state: dict) -> dict:
    """range id -> (home region, region of each copy), for every range that holds a customer."""
    out = {}
    for region in state["regions"]:
        for name, _ in state["people"][region]:
            row = ctx.query(f"SELECT range_id, replica_localities FROM [SHOW RANGE FROM TABLE residency.customers FOR ROW "
                            f"({_literal(region)}, (SELECT id FROM residency.customers WHERE crdb_region = "
                            f"{_literal(region)} AND name = {_literal(name)}))]", one=True)
            if row and len(row) > 1 and isinstance(row[1], (list, tuple)):
                out[row[0]] = (region, [loc.split("region=", 1)[1].split(",", 1)[0] for loc in row[1] if "region=" in loc])
    return out


def _where(home: str, copies: list[str]) -> str:
    """'3 in eu-west-1, 2 outside': copies at home, and how many sit in other regions."""
    if not copies:
        return "unknown"
    outside = sum(r != home for r in copies)
    return f"{copies.count(home)} in {home}, {outside or 'none'} outside"


def _placement(ctx: Context, state: dict, settled, attempts: int = 40) -> dict:
    """Copies outside their home region across every customer's range, once `settled(home, copies)` holds for all of
    them (or the attempts run out: the result then says what it sees), and where one customer per region lives."""
    for _ in range(attempts):
        ranges = _ranges(ctx, state)
        if ranges and all(settled(home, copies) for home, copies in ranges.values()):
            break
        ctx.sleep(3)
    outside = sum(sum(r != home for r in copies) for home, copies in ranges.values())
    first = {}
    for home, copies in ranges.values():
        first.setdefault(home, copies)
    result = {"Copies outside their home region": str(outside), "Data ranges checked": len(ranges)}
    for region in state["regions"]:
        name, city = state["people"][region][0]
        result[f"{name} ({city})"] = _where(region, first.get(region, []))
    return result


def _write(ctx: Context, region: str, name: str, seconds: float = 6) -> str:
    """One write to a customer's row, given up after `seconds` (a row whose region is down would wait for it)."""
    conn = None
    try:
        conn = ctx.connect()
        with abandon_after(conn, seconds):
            with conn.cursor() as cur:
                cur.execute(f"UPDATE residency.customers SET last_seen = now() WHERE crdb_region = {_literal(region)} "
                            f"AND name = {_literal(name)}")
        return "committed"
    except Exception:
        return f"waits for {region}"
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _domicile_start(ctx: Context, state: dict) -> dict:
    _ensure_payments(ctx)
    regions = state["regions"]
    rows = ", ".join(f"({_literal(r)}, {_literal(name)}, {_literal(city)})" for r in regions for name, city in state["people"][r])
    ctx.run(["DROP DATABASE IF EXISTS residency CASCADE",
             f"CREATE DATABASE residency PRIMARY REGION {_ident(regions[0])} REGIONS "
             f"{', '.join(_ident(r) for r in regions[1:])} SURVIVE ZONE FAILURE",
             "USE residency",  # crdb_internal_region resolves in the current database: it must be residency's
             "CREATE TABLE residency.customers (crdb_region crdb_internal_region NOT NULL, id UUID NOT NULL DEFAULT "
             "gen_random_uuid(), name STRING NOT NULL, city STRING NOT NULL, last_seen TIMESTAMPTZ NOT NULL DEFAULT now(), "
             "PRIMARY KEY (crdb_region, id)) LOCALITY REGIONAL BY ROW",
             f"INSERT INTO residency.customers (crdb_region, name, city) VALUES {rows}"])
    return _placement(ctx, state, lambda home, copies: _default_placed(home, copies, regions))


def _default_placed(home: str, copies: list[str], regions: list[str]) -> bool:
    """The default once placed: three copies at home and one read-only copy per other region, for fast reads there.
    Which region holds each read-only copy can take minutes to even out; the count is what matters here."""
    return copies.count(home) == 3 and len(copies) == 3 + len(regions) - 1


def _domicile_restrict(ctx: Context, state: dict) -> dict:
    # v25 needs the session setting for super regions; v26 accepts it and no longer needs it
    ctx.run(["SET enable_super_regions = 'on'"] +
            [f'ALTER DATABASE residency ADD SUPER REGION "{state["supers"][r]}" VALUES {_ident(r)}' for r in state["regions"]],
            idempotent=False)
    return _placement(ctx, state, lambda home, copies: len(copies) == 3 and all(r == home for r in copies))


def _domicile_node_down(ctx: Context, state: dict) -> dict:
    ctx.chaos.act("crash", "node", state["victim"], confirm=True)
    ctx.sleep(2)
    name, abroad = state["abroad_customer"], state["abroad"]
    ranges = {k: v for k, v in _ranges(ctx, state).items() if v[0] == abroad}
    return {f"Write to {name} with {state['victim']} down": _write(ctx, abroad, name, seconds=20),
            f"Copies of {state['abroad_label']}'s customers outside it": str(sum(r != abroad for _, c in ranges.values() for r in c))}


def _domicile_region_down(ctx: Context, state: dict) -> dict:
    ctx.chaos.act("outage", "region", state["abroad"], confirm=True)
    ctx.workload.mark_failure()
    # Losing a region stalls every region for a few seconds while the leaders it held are re-elected. Payments
    # measure that; each customer is written once it is over, so a healthy region's customer is never reported as
    # waiting for their region.
    payments = _wait_recovery(ctx)
    writes = {f"{state['people'][r][0][0]} ({r})": _write(ctx, r, state["people"][r][0][0]) for r in state["regions"]}
    return {**writes, "Payments": f"recovered in {payments['Recovery time']}, {payments['Money lost']} lost"}


def _domicile_region_back(ctx: Context, state: dict) -> dict:
    abroad, name = state["abroad"], state["abroad_customer"]
    ctx.chaos.act("recover", "region", abroad)
    for _ in range(40):
        if _write(ctx, abroad, name) == "committed":
            return {f"{name} ({abroad})": "committed again"}
        ctx.sleep(3)
    return {f"{name} ({abroad})": "still waiting"}


def _domicile_reset(ctx: Context, state: dict) -> None:
    ctx.chaos.recover_all()
    ctx.run("DROP DATABASE IF EXISTS residency CASCADE")


DOMICILE = Scenario(
    id="data-domicile", title="Data domiciling: keep each customer's data at home", tier=1, min_regions=3, min_nodes=6,
    check=_residency_check,
    summary="One table, customers in three regions: super regions keep every copy of a customer's data in their home "
            "region; losing a region pauses only its own customers.",
    prepare=_domicile_prepare, reset=_domicile_reset,
    steps=(
        Step("Create customers", "One table, customers in {region_list}. By default each row also keeps a read-only copy "
             "in every other region.", _domicile_start,
             "Copies outside their home region: one per other region, for fast reads everywhere"),
        Step("Keep data at home", "Each region is its own super region: a customer's data never leaves their region.",
             _domicile_restrict, "Copies outside their home region: 0, across every range that holds a customer"),
        Step("Lose a node in {abroad}", "A node in {abroad} goes down. {abroad_customer}'s row is still written there.",
             _domicile_node_down, "Two of three copies are still at home, one per zone. Nothing is copied abroad to "
             "replace the third."),
        Step("Bring {victim} back", "{victim} rejoins and the third copy is home again", _recover_node("victim"),
             "Three copies in {abroad} again"),
        Step("Lose all of {abroad}", "All of {abroad} goes dark. Its customers wait for it; everyone else keeps working.",
             _domicile_region_down, "No copy of {abroad_label}'s customers exists elsewhere, by design. Payments run in "
             "every region and recover in seconds."),
        Step("Bring {abroad} back", "{abroad} is back and its customers can be written again", _domicile_region_back,
             "The trade: domiciled rows survive a node or a zone, not the loss of their whole region"),
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
    # Under ZONE survival a transfer to a customer in a lost region waits for that region, and each Payments worker
    # sends one transfer at a time: a few of those would hold up every region's payments behind them. They pause for
    # the ZONE half; REGION survival brings them back.
    state.setdefault("cross_region_pct", ctx.workload.cross_region_pct)
    ctx.workload.set_cross_region(0)
    _set_goal(ctx, "zone")
    _ensure_payments(ctx)
    took = _wait_voters(ctx, state["primary"])
    return {"Survival goal": "ZONE", "Voting replicas": "3 per partition, all in the partition's home region",
            "Placed in": f"{took:.0f} s",
            "Cross-region transfers": "paused: one to a customer in a lost region would wait for that region"}


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
    took = _wait_spread(ctx)
    pct = state.get("cross_region_pct", ctx.workload.cross_region_pct)
    ctx.workload.set_cross_region(pct)
    return {"Survival goal": "REGION", "Rebalanced in": f"{took:.0f} s", "Cross-region transfers": f"back on: {pct}%"}


def _zone_reset(ctx: Context, state: dict) -> None:
    ctx.chaos.recover_all()
    if "cross_region_pct" in state:  # ended during the ZONE half
        ctx.workload.set_cross_region(state["cross_region_pct"])


def _survive_again(ctx: Context, state: dict) -> dict:
    ctx.chaos.act("outage", "region", state["primary"], confirm=True)
    ctx.workload.mark_failure()
    ctx.sleep(15)
    out = _wait_recovery(ctx, timeout=60)
    out["Transfers in the last 10 s"] = _commits_last(ctx, 10)
    return out


def _zone_check(topo) -> list[str]:
    """Under ZONE survival each region's payments keep all three voters at home: every region that homes them needs
    three nodes, not only the primary."""
    short = [(r.name, sum(n.region == r.name for n in topo.nodes)) for r in topo.regions if not r.tie_breaker]
    short = [(name, count) for name, count in short if count < 3]
    if not short:
        return []
    return [f"needs 3 nodes in every region, since ZONE survival keeps each region's payments at home "
            f"({short[0][0]} has {short[0][1]}): build the residency preset or add nodes"]


ZONE_VS_REGION = Scenario(
    id="zone-vs-region", title="ZONE vs REGION survival", tier=1, min_regions=3, min_nodes=5, check=_zone_check,
    summary="The same region outage, before and after one ALTER DATABASE statement.",
    prepare=lambda ctx, st: _base_state(ctx, st), reset=_zone_reset,
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
    summary="Each customer's row lives in their region: local reads in ~1-2 ms everywhere, and writes commit in the "
            "row's home region.",
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


ALL = [SURVIVE, SURVIVE_ZONE, STRETCH, ZONE_VS_REGION, ELASTIC, RBR, FOLLOWER, GLOBAL, DOMICILE, SKEW, SCHEMA, CDC, SPLIT, TIME_TRAVEL]
