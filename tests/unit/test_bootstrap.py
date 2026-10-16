from pathlib import Path

import pytest

from controlplane.bootstrap import BootstrapError, Statement, bootstrap_statements, run_bootstrap
from controlplane.catalog import load_catalog
from controlplane.topology import load_preset

ROOT = Path(__file__).resolve().parents[2]
CAT = load_catalog(ROOT / "catalog" / "regions.yaml")
KW = dict(default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)


def topo(name):
    return load_preset(ROOT / "presets", name, CAT, **KW)


def sqls(statements):
    return [s.sql for s in statements]


def test_region_survival_configures_both_databases_in_order():
    s = sqls(bootstrap_statements(topo("aws"), time_until_store_dead="1m15s"))
    assert s[0] == "SET CLUSTER SETTING server.time_until_store_dead = '1m15s'"
    assert s[1] == "SET CLUSTER SETTING kv.rangefeed.enabled = true"
    assert s[2] == "SET CLUSTER SETTING storage.max_sync_duration.fatal.enabled = false"
    assert s[3].startswith('UPSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) VALUES ')
    assert "('region', 'us-east-1', 38.9, -77.4)" in s[3] and "('region', 'eu-west-1', 53.3, -6.3)" in s[3]
    assert s[4] == "CREATE DATABASE IF NOT EXISTS chaos"
    for db in ("defaultdb", "chaos"):
        block = [x for x in s if x.startswith(f"ALTER DATABASE {db} ")]
        assert block == [
            f'ALTER DATABASE {db} SET PRIMARY REGION "us-east-1"',
            f'ALTER DATABASE {db} ADD REGION IF NOT EXISTS "us-west-2"',
            f'ALTER DATABASE {db} ADD REGION IF NOT EXISTS "eu-west-1"',
            f'ALTER DATABASE {db} SET SECONDARY REGION "us-west-2"',
            f"ALTER DATABASE {db} SURVIVE REGION FAILURE",
        ]


def test_store_dead_setting_is_optional_because_minimums_vary_by_version():
    first = bootstrap_statements(topo("aws"), time_until_store_dead="1m15s")[0]
    assert first.optional is True


def test_no_survival_goal_skips_multi_region_but_keeps_locations():
    s = sqls(bootstrap_statements(topo("small"), time_until_store_dead="1m15s"))
    assert not [x for x in s if x.startswith("ALTER DATABASE")]
    assert any(x.startswith("UPSERT INTO system.locations") for x in s)
    assert "CREATE DATABASE IF NOT EXISTS chaos" in s


def test_zone_survival_has_no_secondary_region(tmp_path):
    (tmp_path / "z.yaml").write_text("regions:\n  - {name: us-east-1, provider: aws, nodes: 3, primary: true}\n"
                                     "  - {name: us-west-2, provider: aws, nodes: 1, secondary: true}\n", encoding="utf-8")
    s = sqls(bootstrap_statements(load_preset(tmp_path, "z", CAT, **KW), time_until_store_dead="1m15s"))
    assert "ALTER DATABASE defaultdb SURVIVE ZONE FAILURE" in s
    assert not [x for x in s if "SECONDARY" in x]


def test_license_comes_first_with_quotes_escaped():
    s = sqls(bootstrap_statements(topo("aws"), time_until_store_dead="1m15s", license="crl-0-abc", org="O'Brien Labs"))
    assert s[0] == "SET CLUSTER SETTING cluster.organization = 'O''Brien Labs'"
    assert s[1] == "SET CLUSTER SETTING enterprise.license = 'crl-0-abc'"


class FakeCursor:
    def __init__(self, fail_on=()):
        self.executed, self.fail_on = [], fail_on

    def execute(self, sql):
        self.executed.append(sql)
        if sql in self.fail_on:
            raise RuntimeError("nope")

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


def test_optional_failures_become_warnings_and_required_failures_stop():
    cur = FakeCursor(fail_on={"A"})
    warnings = run_bootstrap(FakeConn(cur), [Statement("A", optional=True), Statement("B")], log=lambda m: None)
    assert cur.executed == ["A", "B"] and warnings and "A" in warnings[0]
    with pytest.raises(BootstrapError) as exc:
        run_bootstrap(FakeConn(FakeCursor(fail_on={"B"})), [Statement("B"), Statement("C")], log=lambda m: None)
    assert exc.value.statement.sql == "B"


def test_laptop_disk_stalls_do_not_kill_nodes():
    stmts = bootstrap_statements(topo("classic"), time_until_store_dead="1m15s")
    st = next(s for s in stmts if "max_sync_duration.fatal.enabled" in s.sql)
    assert st.sql.endswith("= false") and st.optional
