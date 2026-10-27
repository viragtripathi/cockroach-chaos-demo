"""End-to-end checks against a real stack. Rebuilds the local cluster: opt in with CHAOS_INTEGRATION=1."""
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import psycopg
import pytest

from controlplane.sqlconn import connect

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[2]
PANEL = f"http://localhost:{os.environ.get('PANEL_PORT', '8088')}"
CLASSIC_REGIONS = {"us-east-1", "us-west-2", "us-east-2"}


def state():
    with urllib.request.urlopen(f"{PANEL}/api/state", timeout=5) as resp:
        return json.load(resp)


def wait_for(predicate, timeout=300):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            snap = state()
            if predicate(snap):
                return snap
        except OSError:
            pass
        time.sleep(2)
    raise AssertionError("condition not met in time")


def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True, check=True).stdout


@pytest.fixture(scope="module")
def classic():
    if os.environ.get("CHAOS_INTEGRATION") != "1":
        pytest.skip("set CHAOS_INTEGRATION=1 (rebuilds the local cluster as the 'classic' preset)")
    subprocess.run([str(ROOT / "run.sh"), "up", "--preset", "classic"], cwd=ROOT, check=True, timeout=1200)
    return state()


def test_every_node_is_live_with_preset_names(classic):
    c = classic["cluster"]
    assert (c["preset"], c["status"], c["live_nodes"], c["total_nodes"]) == ("classic", "healthy", 5, 5)
    assert {r["name"] for r in classic["regions"]} == CLASSIC_REGIONS
    assert [n["name"] for n in classic["nodes"]] == ["us-east-1a-n1", "us-east-1b-n2", "us-west-2a-n1", "us-west-2b-n2", "us-east-2a-n1"]


def test_sql_through_haproxy_sees_the_multi_region_databases(classic):
    with psycopg.connect(host="localhost", port=26257, user="root", dbname="defaultdb", sslmode="disable", autocommit=True) as conn:
        regions = {r[0]: (r[1], r[2]) for r in conn.execute('SELECT region, "primary", secondary FROM [SHOW REGIONS FROM DATABASE defaultdb]')}
        assert set(regions) == CLASSIC_REGIONS
        assert regions["us-east-1"][0] is True and regions["us-west-2"][1] is True
        assert conn.execute("SELECT survival_goal FROM [SHOW SURVIVAL GOAL FROM DATABASE chaos]").fetchone()[0] == "region"
        assert conn.execute("SHOW CLUSTER SETTING kv.rangefeed.enabled").fetchone()[0] is True


def test_map_locations_were_written(classic):
    with connect("localhost") as conn:
        count = conn.execute('SELECT count(*) FROM system.locations WHERE "localityKey" = %s AND "localityValue" = ANY(%s)',
                             ("region", list(CLASSIC_REGIONS))).fetchone()[0]
    assert count == 3


def test_db_console_is_served_through_the_load_balancer(classic):
    with urllib.request.urlopen("http://localhost:8080/health", timeout=5) as resp:
        assert resp.status == 200


def test_control_plane_restart_adopts_the_running_nodes(classic):
    names = [n["name"] for n in classic["nodes"]]
    before = docker("inspect", "-f", "{{.Id}}", *names).split()
    docker("restart", "crdb-chaos-control-plane")
    snap = wait_for(lambda s: (s.get("operation") or {}).get("kind") == "adopt"
                    and s["operation"]["status"] == "done" and (s.get("cluster") or {}).get("status") == "healthy")
    assert snap["cluster"]["preset"] == "classic"
    assert docker("inspect", "-f", "{{.Id}}", *names).split() == before


# -- chaos, elasticity and traffic (milestones 2-4) --

def call(method, path, body=None):
    req = urllib.request.Request(f"{PANEL}{path}", method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"null")


def node(snap, name):
    return next(n for n in snap["nodes"] if n["name"] == name)


def tc(name):
    return subprocess.run(["docker", "run", "--rm", "--network", f"container:{name}", "--entrypoint", "tc",
                           "crdb-chaos-control-plane:local", "qdisc", "show", "dev", "eth0"], capture_output=True, text=True).stdout


def op_id():
    return (state().get("operation") or {}).get("id")


def wait_op_done(previous, timeout=900):
    """Wait for an operation newer than `previous` to finish (the snapshot lags the API by up to 1.5 s)."""
    return wait_for(lambda s: s["operation"] and s["operation"]["id"] != previous and s["operation"]["status"] != "running",
                    timeout)["operation"]


def test_crash_and_recover_a_node(classic):
    assert call("POST", "/api/chaos", {"action": "crash", "kind": "node", "value": "us-west-2b-n2"})[0] == 200
    wait_for(lambda s: node(s, "us-west-2b-n2")["liveness"] == "dead", 60)
    assert call("POST", "/api/chaos", {"action": "recover", "kind": "node", "value": "us-west-2b-n2"})[0] == 200
    wait_for(lambda s: node(s, "us-west-2b-n2")["liveness"] == "live", 120)


def test_partition_heals_with_the_same_ip_and_shaping_reapplied(classic):
    call("POST", "/api/chaos", {"action": "partition", "kind": "node", "value": "us-east-2a-n1"})
    wait_for(lambda s: node(s, "us-east-2a-n1")["partitioned"], 30)
    call("POST", "/api/chaos", {"action": "recover", "kind": "node", "value": "us-east-2a-n1"})
    snap = wait_for(lambda s: not node(s, "us-east-2a-n1")["partitioned"] and node(s, "us-east-2a-n1")["liveness"] == "live", 120)
    assert node(snap, "us-east-2a-n1")["ip"] == "10.77.3.11"
    assert "netem" in tc("us-east-2a-n1") and "delay" in tc("us-east-2a-n1")


def test_brownout_adds_delay_and_recover_removes_it(classic):
    call("POST", "/api/chaos", {"action": "brownout", "kind": "node", "value": "us-west-2a-n1", "delay_ms": 700})
    assert "delay 7" in tc("us-west-2a-n1")
    wait_for(lambda s: (node(s, "us-west-2a-n1")["brownout"] or {}).get("delay_ms") == 700, 15)
    call("POST", "/api/chaos", {"action": "recover", "kind": "node", "value": "us-west-2a-n1"})
    assert "delay 7" not in tc("us-west-2a-n1")


def test_isolate_a_region_then_recover(classic):
    assert call("POST", "/api/chaos", {"action": "isolate", "kind": "region", "value": "us-east-2"})[0] == 200
    assert "loss 100%" in tc("us-east-2a-n1")
    call("POST", "/api/chaos", {"action": "recover", "kind": "region", "value": "us-east-2"})
    assert "loss 100%" not in tc("us-east-2a-n1")
    wait_for(lambda s: s["cluster"]["status"] == "healthy", 180)


def test_the_guard_asks_before_exceeding_the_survival_goal(classic):
    status, body = call("POST", "/api/chaos", {"action": "outage", "kind": "region", "value": "us-east-1"})
    assert status == 200
    status, body = call("POST", "/api/chaos", {"action": "outage", "kind": "region", "value": "us-west-2"})
    assert status == 409 and body["detail"]["needs_confirm"]
    call("POST", "/api/chaos", {"action": "recover", "kind": "all", "value": ""})
    wait_for(lambda s: s["cluster"]["live_nodes"] == 5, 180)


def test_add_and_decommission_a_node(classic):
    prev = op_id()
    assert call("POST", "/api/cluster/nodes", {"region": "us-east-2"})[0] == 202
    assert wait_op_done(prev)["status"] == "done"
    snap = wait_for(lambda s: any(n["name"] == "us-east-2b-n2" and n["liveness"] == "live" for n in s["nodes"]), 120)
    assert snap["cluster"]["total_nodes"] == 6
    prev = op_id()
    assert call("DELETE", "/api/cluster/nodes/us-east-2b-n2")[0] == 202
    op = wait_op_done(prev)
    assert op["status"] == "done", op
    assert state()["cluster"]["total_nodes"] == 5


def test_add_and_remove_a_region(classic):
    wait_for(lambda s: not s["operation"] or s["operation"]["status"] != "running", 900)
    prev = op_id()
    assert call("POST", "/api/cluster/regions", {"name": "eu-west-1", "provider": "aws", "nodes": 1})[0] == 202
    assert wait_op_done(prev)["status"] == "done"
    with psycopg.connect(host="localhost", port=26257, user="root", dbname="defaultdb", sslmode="disable", autocommit=True) as conn:
        assert "eu-west-1" in {r[0] for r in conn.execute("SELECT region FROM [SHOW REGIONS FROM DATABASE defaultdb]")}
    prev = op_id()
    assert call("DELETE", "/api/cluster/regions/eu-west-1")[0] == 202
    op = wait_op_done(prev)
    assert op["status"] == "done", op
    assert {r["name"] for r in state()["regions"]} == CLASSIC_REGIONS


def test_payments_invariant_and_a_raw_client_survive_a_region_outage(classic):
    wait_for(lambda s: (not s["operation"] or s["operation"]["status"] != "running") and s["cluster"]["status"] == "healthy", 900)
    with psycopg.connect(host="localhost", port=26257, user="root", dbname="defaultdb", sslmode="disable", autocommit=True) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS it_probe (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), at TIMESTAMPTZ DEFAULT now())")
    assert call("POST", "/api/workload/start")[0] == 200
    time.sleep(15)
    call("POST", "/api/chaos", {"action": "outage", "kind": "region", "value": "us-west-2"})
    committed, deadline = 0, time.time() + 30
    while time.time() < deadline:  # a plain psycopg client with simple retries, like Banko's loader
        try:
            with psycopg.connect(host="localhost", port=26257, user="root", dbname="defaultdb", sslmode="disable",
                                 autocommit=True, connect_timeout=5) as conn:
                conn.execute("INSERT INTO it_probe DEFAULT VALUES")
                committed += 1
        except psycopg.Error:
            time.sleep(0.5)
    assert committed > 10
    snap = wait_for(lambda s: s["workload"]["recovery_s"] is not None, 90)
    call("POST", "/api/chaos", {"action": "recover", "kind": "all", "value": ""})
    snap = wait_for(lambda s: s["workload"]["last_check"] and s["workload"]["last_check"] > time.time() - 6, 30)
    assert snap["workload"]["money_lost"] == 0
    call("POST", "/api/workload/stop")
    wait_for(lambda s: s["cluster"]["live_nodes"] == 5, 180)
