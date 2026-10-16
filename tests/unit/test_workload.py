import psycopg
import pytest

from controlplane.workload import ACCOUNTS, TOTAL, Stats, Workload, classify, recovery_time


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

    def execute(self, sql, params=None):
        self.db.sql.append(sql)
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
        self.sql, self.count, self.total, self.id_type = [], count, total, None

    def connect(self, host, **kw):
        db = self

        class Conn:
            autocommit = True

            def cursor(self):
                return FakeCursor(db)

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

    def _open(self):
        self.opened += 1
        return object(), "us-east-1"

    def _transfer(self, conn, tid, a, b, amount):
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
