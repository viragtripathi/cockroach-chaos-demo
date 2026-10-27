import random

import psycopg
import pytest

from controlplane.workload import ACCOUNTS, MAX_RETRIES, TOTAL, Stats, Workload, classify, recovery_time


class SqlState(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.sqlstate = code


@pytest.mark.parametrize("exc, kind", [
    (SqlState("40001"), "serialization"),
    (SqlState("40003"), "ambiguous"),
    (SqlState("57014"), "timeout"),
    (psycopg.OperationalError("server closed the connection unexpectedly"), "connection"),
    (SqlState("XX000"), "other"),
])
def test_errors_are_classified(exc, kind):
    assert classify(exc) == kind


def test_stats_roll_up_per_second_with_latency_percentiles():
    s = Stats(clock=lambda: 10.4)
    for ms in range(1, 101):
        s.commit(ms, "us-east-1", now=10.4)
    s.retry(now=10.4)
    s.error("connection", now=10.4)
    snap = s.window(now=11.5)
    sec = snap["history"][-1]
    assert sec["commits"] == 100 and sec["retries"] == 1 and sec["errors"] == 1
    assert sec["regions"] == {"us-east-1": 100}
    assert snap["p50_ms"] == pytest.approx(50, abs=1) and snap["p99_ms"] == pytest.approx(99, abs=1)
    assert snap["p95_ms"] == pytest.approx(95, abs=1)
    assert snap["errors_total"] == {"connection": 1}


def test_recovery_time_is_first_2s_window_back_at_80_percent_of_baseline():
    history = [{"ts": t, "commits": 100} for t in range(0, 10)]           # baseline 100/s
    history += [{"ts": t, "commits": 0} for t in range(10, 16)]            # outage at t=10
    history += [{"ts": t, "commits": 90} for t in range(16, 20)]           # back
    assert recovery_time(history, failure_at=10.0) == 7.0                  # window [16,17] ends at 17
    assert recovery_time(history[:14], failure_at=10.0) is None            # not recovered yet
    assert recovery_time([{"ts": 0, "commits": 0}], failure_at=0.5) is None


class FakeCursor:
    def __init__(self, db):
        self.db = db

    @property
    def rowcount(self):
        return self.db.rowcount

    def execute(self, sql, params=None):
        self.db.sql.append(sql)
        self.db.calls.append((sql, params))
        self.last = sql

    def fetchall(self):
        if "SELECT id FROM accounts" in self.last:
            return [(f"00000000-0000-0000-0000-{i:012d}",) for i in range(1, 6)]
        return []

    def fetchone(self):
        if "information_schema" in self.last:
            return self.db.id_type
        if "count(*)" in self.last:
            return (self.db.count,)
        if "sum(balance)" in self.last:
            return (self.db.total,)
        if "gateway_region" in self.last:
            return ("us-east-1",)
        return None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class FakeDB:
    def __init__(self, count=0, total=TOTAL):
        self.sql, self.calls, self.count, self.total, self.id_type = [], [], count, total, None
        self.hosts, self.refuse = [], set()  # every host connected to; hosts whose connection is refused
        self.rowcount = 1  # what every UPDATE reports

    def cursor(self):
        return FakeCursor(self)

    def connect(self, host, **kw):
        db = self
        self.hosts.append(host)
        if host in self.refuse:
            raise psycopg.OperationalError("connection refused")

        class Conn:
            autocommit = True

            def cursor(self):
                return db.cursor()

            def transaction(self):
                import contextlib
                return contextlib.nullcontext()

            def close(self):
                pass

        return Conn()


def test_setup_creates_and_seeds_accounts_only_when_needed():
    db = FakeDB(count=0)
    w = Workload(db.connect, host="haproxy")
    w.setup()
    assert any("CREATE TABLE IF NOT EXISTS accounts" in s for s in db.sql)
    assert any("generate_series(1, 10000)" in s for s in db.sql)
    db2 = FakeDB(count=ACCOUNTS)
    Workload(db2.connect, host="haproxy").setup()
    assert not any("generate_series" in s for s in db2.sql)


def test_invariant_check_reports_money_lost():
    db = FakeDB(count=ACCOUNTS, total=TOTAL - 250)
    w = Workload(db.connect, host="haproxy")
    w.check_invariant()
    st = w.status()
    assert st["balance"] == TOTAL - 250 and st["money_lost"] == 250 and st["invariant_ok"] is False


def test_one_transfer_records_its_id_and_moves_money_inside_one_transaction():
    db = FakeDB(count=ACCOUNTS)
    w = Workload(db.connect, host="haproxy")
    conn = db.connect("haproxy")
    w._transfer(conn, "t-1", "a", "b", 5)
    assert db.sql[0].startswith("INSERT INTO transfers")
    assert len([s for s in db.sql if s.startswith("UPDATE accounts")]) == 2


class ScriptedWorkload(Workload):
    """Each transfer attempt raises the next scripted error (None = success)."""
    def __init__(self, script):
        super().__init__(FakeDB().connect, host="haproxy", sleep=lambda s: None)
        self.script, self.ids, self.opened = list(script), [], 0
        self._stop.wait = lambda t=None: False

    def _open(self, state):
        self.opened += 1
        return object(), "us-east-1"

    def _transfer(self, conn, tid, a, b, amount, **kw):
        self.ids.append(tid)
        err = self.script.pop(0)
        if err:
            raise err


def test_every_transient_error_class_is_retried_with_the_same_transfer_id():
    w = ScriptedWorkload([SqlState("40001"), SqlState("40003"), SqlState("08006"), SqlState("57P01"), SqlState("57014"), None])
    assert w._send({}, "a", "b", 5, __import__("random").Random(1)) == "committed"
    assert len(set(w.ids)) == 1 and len(w.ids) == 6
    assert w.opened == 5  # serialization keeps the connection; every maybe-committed error reconnects
    errors = w.stats.window(now=1e12)["errors_total"]
    assert errors == {"ambiguous": 1, "connection": 2, "timeout": 1}


def test_a_duplicate_after_an_ambiguous_commit_means_it_was_already_applied():
    w = ScriptedWorkload([SqlState("40003"), SqlState("23505")])
    assert w._send({}, "a", "b", 5, __import__("random").Random(1)) == "deduplicated"
    assert w.status()["deduplicated"] == 1 and w.status()["failed_total"] == 0


def test_non_retryable_errors_fail_the_transfer_once():
    w = ScriptedWorkload([SqlState("42601")])
    assert w._send({}, "a", "b", 5, __import__("random").Random(1)) == "other"
    assert w.status()["failed_total"] == 1


def test_configure_validates_ranges():
    w = Workload(FakeDB().connect, host="haproxy")
    with pytest.raises(ValueError):
        w.configure(workers=0, rate=100)
    w.configure(workers=4, rate=50)
    assert (w.status()["workers"], w.status()["rate"]) == (4, 50)


def test_history_has_a_row_for_every_second_even_without_commits():
    s = Stats(clock=lambda: 0)
    s.commit(5, "r", now=10.2)
    s.commit(5, "r", now=14.2)
    secs = [h["ts"] for h in s.window(now=16.0)["history"]]
    assert secs == [10, 11, 12, 13, 14, 15]
    assert s.window(now=16.0)["history"][2]["commits"] == 0


def test_accounts_use_uuid_keys_and_old_integer_tables_are_replaced():
    db = FakeDB(count=0)
    db.id_type = ("INT8",)
    w = Workload(db.connect, host="haproxy")
    w.setup()
    assert "DROP TABLE accounts" in db.sql
    assert any("id UUID PRIMARY KEY DEFAULT gen_random_uuid()" in s for s in db.sql)
    assert len(w._ids) == 5


def test_recovery_uses_the_baseline_captured_at_failure_even_after_it_scrolls_out():
    history = [{"ts": t, "commits": 0} for t in range(200, 300)] + [{"ts": t, "commits": 90} for t in range(300, 303)]
    assert recovery_time(history, failure_at=190.0, baseline=100) == 111.0
    assert recovery_time(history, failure_at=190.0) is None  # without the stored baseline it would be stuck


def test_stopping_clears_a_pending_recovery_measurement():
    w = Workload(FakeDB().connect, host="haproxy")
    w.running = True
    w.mark_failure(at=5.0)
    w.stop()
    assert w.status()["failure_at"] is None and w.status()["recovering"] is False


def test_stats_report_latency_per_home_region_and_for_cross_region_transfers():
    s = Stats(clock=lambda: 10.4)
    for ms in range(60, 70):
        s.commit(ms, "us-east-1", now=10.4, home="us-east-1")
    for ms in range(90, 100):
        s.commit(ms, "eu-west-1", now=10.4, home="eu-west-1")
    s.commit(150, "us-east-1", now=10.4, home="us-east-1", cross=True)
    snap = s.window(now=11.5)
    assert snap["by_home"]["us-east-1"]["p50_ms"] == pytest.approx(65, abs=1)
    assert snap["by_home"]["eu-west-1"]["p50_ms"] == pytest.approx(95, abs=1)
    assert snap["by_home"]["us-east-1"]["tps"] == 11 and snap["by_home"]["eu-west-1"]["tps"] == 10
    assert snap["cross_region"] == {"p50_ms": 150, "p95_ms": 150, "tps": 1}
    assert Stats().window(now=5)["by_home"] == {} and Stats().window(now=5)["cross_region"]["tps"] == 0


class HomedDB(FakeDB):
    """A multi-region chaos database. layout: {table: (locality, create_stmt)} for the tables that exist."""
    def __init__(self, regions=("us-east-1", "us-west-2", "eu-west-1"), layout=None, counts=None, holder_home=True):
        super().__init__(count=ACCOUNTS)
        self.regions, self.layout, self.counts, self.holder_home = list(regions), layout or {}, counts, holder_home

    def cursor(self):
        return HomedCursor(self)


class HomedCursor(FakeCursor):
    def fetchall(self):
        last = self.last
        if "SHOW REGIONS FROM DATABASE" in last:
            return [("chaos", r, i == 0, False, []) for i, r in enumerate(self.db.regions)]
        if "SHOW TABLES" in last:
            return [(t, loc) for t, (loc, _) in self.db.layout.items()]
        if "crdb_region, count(*)" in last:
            return list((self.db.counts or {}).items())
        if "SELECT crdb_region, id FROM accounts" in last:
            return [(r, f"00000000-0000-0000-000{i}-{k:012d}") for i, r in enumerate(self.db.regions) for k in range(1, 4)]
        return super().fetchall()

    def fetchone(self):
        last = self.last
        if "SHOW CREATE TABLE" in last:
            name = last.split()[-1]
            return (name, self.db.layout[name][1])
        if "SHOW RANGE FROM TABLE" in last:
            region = last.split("FOR ROW ('")[1].split("'")[0]
            return (42, 7 if self.db.holder_home else 9, [7, 8, 9],
                    [f"region={region},zone=a", f"region={region},zone=b", "region=elsewhere,zone=a"])
        return super().fetchone()


HOMED = ("REGIONAL BY ROW", "CREATE TABLE accounts (... CONSTRAINT accounts_pkey PRIMARY KEY (crdb_region ASC, id ASC) ...)")
PLAIN = ("REGIONAL BY TABLE IN PRIMARY REGION", "CREATE TABLE accounts (... PRIMARY KEY (id ASC) ...)")
FULL = {"us-east-1": 3334, "us-west-2": 3333, "eu-west-1": 3333}


def test_a_multi_region_database_gets_tables_keyed_by_region():
    db = HomedDB(layout={"accounts": PLAIN, "transfers": PLAIN})
    w = Workload(db.connect, host="haproxy", cluster=dict)
    w.setup()
    joined = "\n".join(db.sql)
    assert joined.index("DROP TABLE accounts") < joined.index("CREATE TABLE IF NOT EXISTS accounts")
    assert "DROP TABLE transfers" in joined
    assert "PRIMARY KEY (crdb_region, id)) LOCALITY REGIONAL BY ROW" in joined
    assert "WITH (ttl_expire_after = '1 hour') LOCALITY REGIONAL BY ROW" in joined
    assert w.homed and w.regions == ["us-east-1", "us-west-2", "eu-west-1"] and w.primary == "us-east-1"
    assert sorted(w._homes) == ["eu-west-1", "us-east-1", "us-west-2"] and len(w._ids) == 9


def test_homed_tables_are_kept():
    db = HomedDB(layout={"accounts": HOMED, "transfers": HOMED}, counts=FULL)
    Workload(db.connect, host="haproxy", cluster=dict).setup()
    assert not any(s.startswith("DROP TABLE") or s.startswith("TRUNCATE") for s in db.sql)


def test_accounts_are_seeded_round_robin_with_an_exact_total():
    db = HomedDB(layout={"accounts": HOMED, "transfers": HOMED}, counts={"us-east-1": 10000})
    Workload(db.connect, host="haproxy", cluster=dict).setup()
    seeds = [p for s, p in db.calls if s.startswith("INSERT INTO accounts (crdb_region, balance)")]
    assert seeds == [("us-east-1", 1000, 3334), ("us-west-2", 1000, 3333), ("eu-west-1", 1000, 3333)]
    assert "TRUNCATE accounts" in db.sql


def test_partition_leases_are_moved_home_when_they_are_elsewhere():
    db = HomedDB(layout={"accounts": HOMED, "transfers": HOMED}, counts=FULL, holder_home=False)
    Workload(db.connect, host="haproxy", cluster=dict).setup()
    assert db.sql.count("ALTER RANGE 42 RELOCATE LEASE TO 7") == 6  # two tables, three regions
    db2 = HomedDB(layout={"accounts": HOMED, "transfers": HOMED}, counts=FULL)
    Workload(db2.connect, host="haproxy", cluster=dict).setup()
    assert not any("RELOCATE LEASE" in s for s in db2.sql)


def test_a_single_region_database_keeps_the_plain_layout():
    db = HomedDB(regions=())
    w = Workload(db.connect, host="haproxy", cluster=dict)
    w.setup()
    assert any("id UUID PRIMARY KEY DEFAULT gen_random_uuid()" in s for s in db.sql) and not w.homed


LIVE = {"regions": [{"name": "us-east-1", "location": [38.9, -77.4], "primary": True},
                    {"name": "us-west-2", "location": [45.8, -119.7], "primary": False},
                    {"name": "eu-west-1", "location": [53.3, -6.3], "primary": False}],
        "nodes": [{"name": "us-east-1a-n1", "region": "us-east-1", "ip": "10.77.1.11", "liveness": "live", "runtime": "running", "partitioned": False, "isolated": False},
                  {"name": "us-west-2a-n1", "region": "us-west-2", "ip": "10.77.2.11", "liveness": "live", "runtime": "running", "partitioned": False, "isolated": False},
                  {"name": "eu-west-1a-n1", "region": "eu-west-1", "ip": "10.77.3.11", "liveness": "live", "runtime": "running", "partitioned": False, "isolated": False}]}


def homed_workload(snapshot, refuse=()):
    """A workload whose setup already ran on a 3-region database; connecting to a host in `refuse` fails."""
    db = HomedDB(layout={"accounts": HOMED, "transfers": HOMED}, counts=FULL)
    db.refuse = set(refuse)
    w = Workload(db.connect, host="haproxy", cluster=lambda: snapshot, sleep=lambda s: None)
    w.setup()
    db.hosts.clear()
    return w, db


def test_a_homed_worker_connects_to_its_own_region():
    w, db = homed_workload(LIVE)
    state = {"conn": None, "home": "eu-west-1"}
    conn, region = w._open(state)
    assert db.hosts == ["10.77.3.11"] and region == "eu-west-1" and state["failover"] is False
    assert "haproxy" not in db.hosts


def test_a_worker_fails_over_to_the_nearest_live_region_and_comes_home():
    down = dict(LIVE, nodes=[dict(n, liveness="dead") if n["region"] == "eu-west-1" else n for n in LIVE["nodes"]])
    snap = {"now": down}
    w, db = homed_workload(None)
    w.cluster = lambda: snap["now"]
    state = {"conn": None, "home": "eu-west-1"}
    _, region = w._open(state)
    assert region == "us-east-1" and state["failover"] is True  # Ireland's nearest voter region is Virginia
    assert w.status()["homing"]["failover"] == {"eu-west-1": "us-east-1"}
    snap["now"] = LIVE
    _, region = w._open(state)
    assert region == "eu-west-1" and w.status()["homing"]["failover"] == {"eu-west-1": None}


def test_a_refused_node_is_skipped_for_the_next_one_in_the_region():
    two = dict(LIVE, nodes=LIVE["nodes"] + [dict(LIVE["nodes"][0], name="us-east-1b-n2", ip="10.77.1.12")])
    w, db = homed_workload(two, refuse={"10.77.1.11"})
    _, region = w._open({"conn": None, "home": "us-east-1"})
    assert region == "us-east-1" and db.hosts[-1] == "10.77.1.12"


def test_a_homed_worker_with_no_snapshot_counts_a_connection_error():
    w, db = homed_workload({})
    w._stop.wait = lambda t=None: False
    w._transfer = lambda *a, **k: None
    out = w._send({"conn": None, "home": "us-east-1"}, "a", "b", 5, random.Random(1), a_home="us-east-1", b_home="us-east-1")
    assert out == "connection" and w.stats.window(now=1e12)["errors_total"] == {"connection": MAX_RETRIES}


def test_a_homed_transfer_names_every_row_by_region():
    w, db = homed_workload(LIVE)
    w._transfer(db.connect("x"), "t-1", "a", "b", 5, home="us-east-1", a_home="us-east-1", b_home="eu-west-1")
    assert db.calls[-3] == ("INSERT INTO transfers (crdb_region, id, src, dst, amount) VALUES (%s, %s, %s, %s, %s)", ("us-east-1", "t-1", "a", "b", 5))
    assert db.calls[-2] == ("UPDATE accounts SET balance = balance - %s WHERE crdb_region = %s AND id = %s", (5, "us-east-1", "a"))
    assert db.calls[-1] == ("UPDATE accounts SET balance = balance + %s WHERE crdb_region = %s AND id = %s", (5, "eu-west-1", "b"))


def test_about_one_transfer_in_ten_crosses_a_region():
    w, _ = homed_workload(LIVE)
    picks = [w._pick(random.Random(i), "us-west-2") for i in range(1000)]  # (a, a_home, b, b_home)
    assert all(a_home == "us-west-2" and a != b for a, a_home, b, b_home in picks)
    cross = sum(b_home != "us-west-2" for _, _, _, b_home in picks)
    assert 60 <= cross <= 140
    w.configure(workers=3, rate=10, cross_region_pct=0)
    assert all(w._pick(random.Random(i), "us-west-2")[3] == "us-west-2" for i in range(200))
    with pytest.raises(ValueError):
        w.configure(workers=3, rate=10, cross_region_pct=51)


def test_status_describes_the_homing():
    w, _ = homed_workload(LIVE)
    h = w.status()["homing"]
    assert h["on"] and h["regions"] == ["us-east-1", "us-west-2", "eu-west-1"] and h["cross_region_pct"] == 10
    assert h["workers"] == {"us-east-1": 16, "us-west-2": 16, "eu-west-1": 16}
    plain = Workload(FakeDB(count=ACCOUNTS).connect, host="haproxy")
    plain.setup()
    assert plain.status()["homing"] == {"on": False, "regions": [], "cross_region_pct": 10, "workers": {}, "failover": {}}


def test_evacuate_moves_a_regions_rows_to_the_primary_and_pauses_a_running_workload():
    w, db = homed_workload(LIVE)
    w.running = True
    w.evacuate("eu-west-1")
    assert ("UPDATE accounts SET crdb_region = %s WHERE crdb_region = %s", ("us-east-1", "eu-west-1")) in db.calls
    assert "TRUNCATE transfers" in db.sql  # transfer rows only dedupe in-flight retries, and Payments is stopped
    assert not any(s.startswith("UPDATE transfers") for s in db.sql)
    assert w.running is False and w.regions == ["us-east-1", "us-west-2"] and "eu-west-1" not in w._homes
    assert w.status()["paused_for"] == "eu-west-1"
    with pytest.raises(RuntimeError, match="paused while eu-west-1 is removed"):
        w.start()
    w.resume("eu-west-1")
    assert w.running is True and w.status()["paused_for"] is None


def test_evacuating_the_primary_moves_rows_to_the_next_region():
    w, db = homed_workload(LIVE)
    w.evacuate("us-east-1")
    assert ("UPDATE accounts SET crdb_region = %s WHERE crdb_region = %s", ("us-west-2", "us-east-1")) in db.calls


def test_evacuate_does_nothing_in_plain_mode():
    db = FakeDB(count=ACCOUNTS)
    w = Workload(db.connect, host="haproxy")
    w.setup()
    w.running = True
    w.evacuate("us-east-1")
    assert not any(s.startswith(("UPDATE", "TRUNCATE")) for s in db.sql)  # a look at the layout is fine
    assert w.running is True and w.status()["paused_for"] is None
    w.resume("us-east-1")
    assert w.running is True


def test_evacuate_works_on_a_workload_that_never_started_in_this_process():
    """After a control-plane restart nothing has called setup(), but the tables still hold homed rows."""
    db = HomedDB(layout={"accounts": HOMED, "transfers": HOMED}, counts=FULL)
    w = Workload(db.connect, host="haproxy", cluster=dict)
    w.evacuate("eu-west-1")
    assert ("UPDATE accounts SET crdb_region = %s WHERE crdb_region = %s", ("us-east-1", "eu-west-1")) in db.calls


def test_evacuating_the_only_region_is_refused_with_a_reason():
    db = HomedDB(regions=("us-east-1",), layout={"accounts": HOMED, "transfers": HOMED}, counts={"us-east-1": 10000})
    w = Workload(db.connect, host="haproxy", cluster=dict)
    with pytest.raises(RuntimeError, match="only region"):
        w.evacuate("us-east-1")
    assert not any(s.startswith(("UPDATE", "TRUNCATE")) for s in db.sql)


def test_a_homed_update_that_hits_no_row_rolls_the_transfer_back():
    w, db = homed_workload(LIVE)
    db.rowcount = 0  # the account moved to another region since the worker chose it
    with pytest.raises(RuntimeError, match="moved"):
        w._transfer(db.connect("x"), "t-1", "a", "b", 5, home="us-east-1", a_home="us-east-1", b_home="us-east-1")


def test_a_stopping_worker_does_not_start_a_transfer_after_it_connects():
    w = ScriptedWorkload([None])
    w._stop.set()
    assert w._send({}, "a", "b", 5, random.Random(1)) == "stopped" and w.ids == []
