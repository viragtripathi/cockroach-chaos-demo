#!/usr/bin/env python3
"""Proof run: drive every feature of the demo against the live local stack and write an evidence report.

    ./run.sh prove            # everything, about an hour (rebuilds the cluster several times)
    ./run.sh prove --quick    # skips the long parts: guided scenarios, tokens, explainer export

Writes proof/index.html (pass/fail, measurements, screenshots per feature) and proof/report.json.
The cluster ends on the default preset (aws, multi-region) with payments stopped.
"""
from __future__ import annotations

import argparse
import html
import http.cookiejar
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "controlplane"))

import psycopg  # noqa: E402  (from the project virtualenv)

PANEL = f"http://localhost:{os.environ.get('PANEL_PORT', '8088')}"
CLI = "docker" if shutil.which("docker") else "podman"
HELPER = "crdb-chaos-control-plane:local"
FORBIDDEN = ("docker", "container", "podman", "localhost")


class Failure(AssertionError):
    pass


def check(condition, message: str) -> None:
    if not condition:
        raise Failure(message)


# -- plumbing ------------------------------------------------------------------------------------------------------

def api(method: str, path: str, body=None, headers=None, opener=None, timeout=90):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(PANEL + path, data=data, method=method,
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with (opener.open if opener else urllib.request.urlopen)(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw and raw[:1] in (b"{", b"[") else raw.decode(errors="replace"))
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw or b"null")
        except ValueError:
            return e.code, raw.decode(errors="replace")


def state() -> dict:
    return api("GET", "/api/state")[1]


def wait_for(predicate, timeout: float, what: str, every: float = 2.0):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as e:  # transient (control plane restarting, node starting)
            last_error = e
        time.sleep(every)
    raise Failure(f"timed out after {timeout:.0f}s waiting for {what}" + (f" ({last_error})" if last_error else ""))


def op_id():
    return (state().get("operation") or {}).get("id")


def wait_op(previous, timeout=1500) -> dict:
    return wait_for(lambda: (lambda s: s["operation"] if s.get("operation") and s["operation"]["id"] != previous
                             and s["operation"]["status"] != "running" else None)(state()), timeout, "the operation to finish")


def healthy(expect_nodes=None) -> dict:
    def ok():
        s = state()
        c = s.get("cluster") or {}
        if c.get("status") == "healthy" and c.get("live_nodes") == c.get("total_nodes") and \
                (expect_nodes is None or c.get("total_nodes") == expect_nodes):
            return s
        return None
    return wait_for(ok, 300, "the cluster to be healthy with every node live")


def node(s: dict, name: str) -> dict:
    return next(n for n in s["nodes"] if n["name"] == name)


def nodes_in(s: dict, region: str) -> list[dict]:
    return [n for n in s["nodes"] if n["region"] == region]


def chaos(action, kind, value="", **extra):
    return api("POST", "/api/chaos", {"action": action, "kind": kind, "value": value, **extra})


def recover_all():
    chaos("recover", "all")


def rebuild(preset: str, model: str = "multi-region", expect_nodes=None) -> dict:
    previous = op_id()
    status, body = api("POST", "/api/cluster/rebuild", {"preset": preset, "model": model})
    check(status == 202, f"rebuild refused: {status} {body}")
    op = wait_op(previous)
    check(op["status"] == "done", f"rebuild failed: {op.get('error')}")
    return healthy(expect_nodes)


def sh(*args, timeout=180, check_rc=True, env=None) -> str:
    proc = subprocess.run(list(args), capture_output=True, text=True, timeout=timeout, env=env, cwd=ROOT)
    if check_rc and proc.returncode != 0:
        raise Failure(f"{' '.join(args[:4])}… failed: {(proc.stderr or proc.stdout).strip()[:400]}")
    return proc.stdout


def sql(query: str, *, db: str = "defaultdb", one=False):
    with psycopg.connect(host="localhost", port=26257, user="root", dbname=db, sslmode="disable", autocommit=True,
                         connect_timeout=10, options="-c allow_unsafe_internals=on") as conn:
        cur = conn.execute(query)
        if cur.description is None:  # DDL and other statements without rows
            return None
        return cur.fetchone() if one else cur.fetchall()


def tc(name: str) -> str:
    return sh(CLI, "run", "--rm", "--network", f"container:{name}", "--entrypoint", "tc", HELPER, "qdisc", "show", "dev", "eth0")


RTT_PY = """
import socket, statistics, sys, time
ip = sys.argv[1]; out = []
for _ in range(5):
    s = socket.socket(); s.settimeout(3); t = time.perf_counter()
    try:
        s.connect((ip, 26257)); out.append((time.perf_counter() - t) * 1000)
    except OSError:
        pass
    finally:
        s.close()
print(round(statistics.median(out), 1) if out else -1)
"""


def rtt(from_node: str, to_ip: str) -> float | None:
    """Median TCP connect time (one round trip) from inside a node's network to another node."""
    out = sh(CLI, "run", "--rm", "--network", f"container:{from_node}", "--entrypoint", "python3", HELPER, "-c", RTT_PY, to_ip)
    value = float(out.strip() or -1)
    return None if value < 0 else value


def compose(env_extra: dict):
    dc = [CLI, "compose"]
    env = {**os.environ, **env_extra}
    sh(*dc, "up", "-d", "control-plane", env=env, timeout=600)
    wait_for(lambda: api("GET", "/api/state")[0] == 200 and (state().get("operation") or {}).get("status") != "running",
             240, "the control plane to come back")


# -- report --------------------------------------------------------------------------------------------------------

class Proof:
    def __init__(self, out: Path, quick: bool):
        self.out, self.quick = out, quick
        self.shots = out / "shots"
        self.shots.mkdir(parents=True, exist_ok=True)
        self.results: list[dict] = []
        self.section = ""
        self.started = time.time()
        self.meta: dict = {}

    def shot(self, name: str) -> str:
        return str(self.shots / f"{name}.png")

    def run(self, title: str, fn, *, long: bool = False):
        if long and self.quick:
            self._record(title, "skip", 0.0, {}, "skipped in --quick mode")
            return
        started = time.time()
        evidence: dict = {}
        status, error = "pass", None
        try:
            fn(evidence)
        except Failure as e:
            status, error = "fail", str(e)
        except Exception as e:
            status, error = "fail", f"{type(e).__name__}: {e}"
            evidence["traceback"] = traceback.format_exc()[-1800:]
        self._record(title, status, time.time() - started, evidence, error)

    def _record(self, title, status, seconds, evidence, error):
        self.results.append({"section": self.section, "title": title, "status": status, "seconds": round(seconds, 1),
                             "evidence": evidence, "error": error})
        mark = {"pass": "✓", "fail": "✗", "skip": "–"}[status]
        print(f"  {mark} {title} ({seconds:.0f}s){'  ' + error if error else ''}", flush=True)
        self.write()

    def write(self):
        summary = {k: sum(r["status"] == k for r in self.results) for k in ("pass", "fail", "skip")}
        report = {"started": self.started, "minutes": round((time.time() - self.started) / 60, 1), "summary": summary,
                  "meta": self.meta, "results": self.results}
        (self.out / "report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        (self.out / "index.html").write_text(render_html(report, self.out), encoding="utf-8")


def _value(v) -> str:
    if isinstance(v, (dict, list)):
        text = json.dumps(v, indent=1, default=str)
        return f"<pre>{html.escape(text[:2500])}{'…' if len(text) > 2500 else ''}</pre>"
    text = str(v)
    if text.endswith(".png") and Path(text).exists():
        rel = Path(text).relative_to(Path(text).parents[1])
        return f'<a href="{html.escape(str(rel))}"><img src="{html.escape(str(rel))}" loading="lazy"></a>'
    return html.escape(text[:1200])


def render_html(report: dict, out: Path) -> str:
    s = report["summary"]
    rows, section = [], None
    for r in report["results"]:
        if r["section"] != section:
            section = r["section"]
            rows.append(f'<tr class="sec"><td colspan="3">{html.escape(section)}</td></tr>')
        ev = "".join(f"<div class='kv'><span>{html.escape(str(k))}</span>{_value(v)}</div>" for k, v in r["evidence"].items())
        err = f"<div class='err'>{html.escape(r['error'])}</div>" if r["error"] else ""
        rows.append(f"<tr class='{r['status']}'><td class='st'>{ {'pass': 'PASS', 'fail': 'FAIL', 'skip': 'SKIP'}[r['status']] }</td>"
                    f"<td><b>{html.escape(r['title'])}</b> <small>{r['seconds']} s</small>{err}{ev}</td></tr>")
    meta = "".join(f"<span>{html.escape(k)}: <b>{html.escape(str(v))}</b></span>" for k, v in report["meta"].items())
    return f"""<!doctype html><html><head><meta charset="utf-8"><title>Proof run · CockroachDB chaos demo</title>
<style>
body{{background:#000;color:#fff;font:15px/1.5 system-ui,sans-serif;margin:0;padding:28px 36px}}
h1{{font-weight:300;font-size:30px;margin:0 0 6px}} .meta span{{margin-right:18px;color:#aaa}}
.sum{{display:flex;gap:14px;margin:18px 0 22px}} .sum div{{padding:10px 18px;border-radius:12px;border:1px solid #333;font-size:22px}}
.p{{color:#aede54;border-color:#aede5466!important}} .f{{color:#ff4d5e;border-color:#ff4d5e66!important}} .k{{color:#888}}
table{{border-collapse:collapse;width:100%}} td{{border-top:1px solid #222;padding:10px 8px;vertical-align:top}}
tr.sec td{{font:600 12px ui-monospace,monospace;letter-spacing:2px;text-transform:uppercase;color:#c8acff;padding-top:22px}}
td.st{{width:64px;font:600 12px ui-monospace,monospace}} tr.pass td.st{{color:#aede54}} tr.fail td.st{{color:#ff4d5e}} tr.skip td.st{{color:#888}}
.err{{color:#ff4d5e;margin:6px 0;font-family:ui-monospace,monospace;font-size:13px}} small{{color:#888}}
.kv{{display:grid;grid-template-columns:220px 1fr;gap:10px;margin-top:4px;font-size:13px}} .kv span{{color:#888}}
pre{{margin:0;white-space:pre-wrap;font-size:12px;color:#ccc}} img{{max-width:520px;border:1px solid #333;border-radius:8px}}
</style></head><body>
<h1>Proof run · CockroachDB chaos demo</h1><div class="meta">{meta}<span>duration: <b>{report['minutes']} min</b></span></div>
<div class="sum"><div class="p">{s['pass']} passed</div><div class="f">{s['fail']} failed</div><div class="k">{s['skip']} skipped</div></div>
<table>{''.join(rows)}</table></body></html>"""


# -- browser -------------------------------------------------------------------------------------------------------

def browser(proof: Proof, plan: list, timeout=240) -> dict:
    path = proof.out / "plan.json"
    path.write_text(json.dumps(plan), encoding="utf-8")
    out = sh("node", str(ROOT / "tools" / "proof-browser.mjs"), str(path), timeout=timeout)
    result = json.loads(out.strip().splitlines()[-1])
    check("error" not in result, f"browser: {result.get('error')}")
    return result


STAGE = f"{PANEL}/"


# -- the checks ----------------------------------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true")
    parser.add_argument("--out", default=str(ROOT / "proof"))
    args = parser.parse_args()
    out = Path(args.out)
    if out.exists():
        shutil.rmtree(out)
    p = Proof(out, args.quick)
    print(f"Proof run ({'quick' if args.quick else 'full'}): writing {out}/index.html", flush=True)

    check(api("GET", "/api/state")[0] == 200, "the control plane isn't answering: run ./run.sh up first")
    p.meta = {"mode": "quick" if args.quick else "full", "runtime": CLI,
              "cockroach": sh(CLI, "image", "inspect", "--format", "{{index .RepoTags 0}}", "docker.io/cockroachdb/cockroach:v26.3.2",
                              check_rc=False).strip() or "v26.3.2",
              "node": sh("node", "--version").strip(), "started": time.strftime("%Y-%m-%d %H:%M")}

    # ---- environment ----
    p.section = "Environment and tests"

    def unit_tests(ev):
        out_ = sh(str(ROOT / ".venv" / "bin" / "pytest"), "-q", "-p", "no:cacheprovider", "tests/unit", timeout=600, check_rc=False)
        last = out_.strip().splitlines()[-1]
        ev["result"] = last
        check(" passed" in last and "failed" not in last, f"unit tests: {last}")
    p.run("Unit test suite passes", unit_tests)

    def preflight(ev):
        out_ = sh("python3", "tools/preflight.py", "--preset-file", "presets/aws.yaml", "--subnet",
                  os.environ.get("CHAOS_SUBNET", "10.77.0.0/16"), "--panel-port", PANEL.rsplit(":", 1)[1], check_rc=False)
        ev["output"] = out_.strip()
        check("✗" not in out_, "preflight reported an error")
    p.run("Preflight checks pass on this machine", preflight)

    # ---- multi-region default ----
    p.section = "Multi-region (default deployment)"
    api("POST", "/api/workload/stop")
    api("PUT", "/api/booth", {"enabled": False, "auto_heal_s": 45})

    def provision(ev):
        started = time.time()
        s = rebuild("aws", "multi-region", expect_nodes=6)
        c = s["cluster"]
        ev["provisioned in"] = f"{time.time() - started:.0f} s"
        ev["regions"] = {r["name"]: len(r["nodes"]) for r in s["regions"]}
        ev["survival goal"], ev["model"] = c["survival_goal"], c["model"]
        check(c["model"] == "multi-region" and c["survival_goal"] == "region", f"unexpected {c['model']} / {c['survival_goal']}")
        check(len(s["regions"]) == 3 and all(len(r["nodes"]) == 2 for r in s["regions"]), "expected 3 regions x 2 nodes")
    p.run("Default preset builds 3 regions x 2 nodes with REGION survival", provision)

    def node_flags(ev):
        s = state()
        for n in s["nodes"]:
            argv = json.loads(sh(CLI, "inspect", n["name"], "--format", "{{json .Args}}"))
            check("--max-go-memory=1GiB" in argv, f"{n['name']} lacks --max-go-memory")
            check(any("max-group-size" in a for a in argv), f"{n['name']} lacks log size caps")
        ev["checked"] = f"{len(s['nodes'])} nodes: --max-go-memory=1GiB, log caps, --locality, --cluster-name"
        ev["example"] = [a for a in argv if a.startswith(("--max-go", "--log", "--locality", "--cluster-name"))]
    p.run("Nodes start with the Go memory limit and bounded logs", node_flags)

    def settings(ev):
        fatal = sql("SHOW CLUSTER SETTING storage.max_sync_duration.fatal.enabled", one=True)[0]
        rangefeed = sql("SHOW CLUSTER SETTING kv.rangefeed.enabled", one=True)[0]
        dead = sql("SHOW CLUSTER SETTING server.time_until_store_dead", one=True)[0]
        locations = sql("SELECT count(*) FROM system.locations WHERE \"localityKey\" = 'region'", one=True)[0]
        ev.update({"disk stall is fatal": fatal, "rangefeeds": rangefeed, "time until store dead": str(dead),
                   "map locations": locations})
        check(fatal is False and rangefeed is True and locations >= 3, "bootstrap settings are not as expected")
    p.run("Bootstrap applied cluster settings and map locations", settings)

    def banko(ev):
        rows = sql('SELECT region, "primary", secondary FROM [SHOW REGIONS FROM DATABASE defaultdb]')
        goals = {db: sql(f"SELECT survival_goal FROM [SHOW SURVIVAL GOAL FROM DATABASE {db}]", one=True)[0] for db in ("defaultdb", "chaos")}
        ev["defaultdb regions"] = [list(r) for r in rows]
        ev["survival goals"] = goals
        check({r[0] for r in rows} == {"us-east-1", "us-west-2", "eu-west-1"}, "defaultdb regions are wrong")
        check(any(r[0] == "us-east-1" and r[1] for r in rows) and any(r[0] == "us-west-2" and r[2] for r in rows),
              "primary/secondary are wrong")
        check(set(goals.values()) == {"region"}, "survival goal is not REGION on both databases")
    p.run("Banko's defaultdb and the chaos database are multi-region", banko)

    def load_balancer(ev):
        one = sql("SELECT 1", one=True)[0]
        console = urllib.request.urlopen("http://localhost:8080/health", timeout=10).status
        stats = urllib.request.urlopen("http://localhost:8404/stats;csv", timeout=10).read().decode()
        up = sum(1 for line in stats.splitlines() if line.startswith("crdb_sql_nodes,") and ",UP," in line
                 and line.split(",")[1] not in ("BACKEND", "FRONTEND"))
        ev.update({"SQL through :26257": one, "DB Console": console, "SQL backends UP": up})
        check(one == 1 and console == 200 and up == 6, "load balancer checks failed")
    p.run("Global load balancer serves SQL and the DB Console", load_balancer)

    def idle_traffic(ev):
        time.sleep(8)
        samples = []
        for _ in range(3):
            samples.append((state()["traffic"]["cluster"] or {}).get("txn_s"))
            time.sleep(2.5)
        ev["tx/s while idle"] = samples
        check(all(v is not None and v < 0.5 for v in samples), "the stage's own polling shows up as client traffic")
    p.run("Idle cluster shows 0 transactions (telemetry is not SQL)", idle_traffic)

    def client_traffic(ev):
        with psycopg.connect(host="localhost", port=26257, user="root", dbname="defaultdb", sslmode="disable", autocommit=True) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS proof_probe (id UUID PRIMARY KEY DEFAULT gen_random_uuid())")
            deadline = time.time() + 8
            n = 0
            while time.time() < deadline:
                conn.execute("INSERT INTO proof_probe DEFAULT VALUES")
                n += 1
        seen = wait_for(lambda: (lambda v: v if v and v > 5 else None)((state()["traffic"]["cluster"] or {}).get("txn_s")),
                        20, "external client traffic to show up", every=1)
        ev.update({"inserts sent by a plain client": n, "cluster tx/s seen": seen})
        sql("DROP TABLE IF EXISTS proof_probe")
    p.run("Traffic from any client (like Banko) is counted", client_traffic)

    from controlplane.catalog import load_catalog
    from controlplane.netem import rtt_ms
    from controlplane.topology import load_preset
    catalog = load_catalog(ROOT / "catalog" / "regions.yaml")
    aws = load_preset(ROOT / "presets", "aws", catalog, default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
    reg = {r.name: r for r in aws.regions}

    def latency(ev):
        s = state()
        east = nodes_in(s, "us-east-1")
        table = {}
        for target in ("us-west-2", "eu-west-1"):
            measured = rtt(east[0]["name"], nodes_in(s, target)[0]["ip"])
            expected = rtt_ms(reg["us-east-1"], reg[target], 1.0)
            table[f"us-east-1 → {target}"] = f"measured {measured} ms, model {expected:.0f} ms"
            check(measured and abs(measured - expected) <= max(12, expected * 0.3), f"{target}: {measured} vs {expected:.0f} ms")
        local = rtt(east[0]["name"], east[1]["ip"])
        table["us-east-1 → us-east-1 (same region)"] = f"measured {local} ms"
        check(local is not None and local < 8, f"same-region round trip is {local} ms")
        ev["round trips"] = table
    p.run("Realistic latency between regions matches their distance", latency)

    def latency_settings(ev):
        s = state()
        src, dst = nodes_in(s, "us-east-1")[0]["name"], nodes_in(s, "eu-west-1")[0]["ip"]
        expected = rtt_ms(reg["us-east-1"], reg["eu-west-1"], 1.0)
        results = {}
        for setting, factor in (("off", 0), ("2", 2), ("realistic", 1)):
            check(api("POST", "/api/cluster/latency", {"setting": setting})[0] == 200, f"latency {setting} refused")
            time.sleep(2)
            results[setting] = rtt(src, dst)
        ev["us-east-1 → eu-west-1"] = {k: f"{v} ms" for k, v in results.items()}
        check(results["off"] < 10, "Off still delays traffic")
        check(abs(results["2"] - 2 * expected) <= 2 * expected * 0.3, "Double isn't about twice the realistic delay")
        check(abs(results["realistic"] - expected) <= expected * 0.3, "Realistic didn't come back")
    p.run("Latency control: Off, Double and back to Realistic", latency_settings)

    # ---- chaos ----
    p.section = "Chaos"

    def crash(ev):
        target = "us-west-2b-n2"
        t0 = time.time()
        check(chaos("crash", "node", target)[0] == 200, "crash refused")
        wait_for(lambda: node(state(), target)["liveness"] == "dead", 90, f"{target} to be detected as dead")
        ev["detected dead after"] = f"{time.time() - t0:.0f} s"
        chaos("recover", "node", target)
        t1 = time.time()
        wait_for(lambda: node(state(), target)["liveness"] == "live", 180, f"{target} to come back")
        ev["back after recover"] = f"{time.time() - t1:.0f} s"
        healthy()
    p.run("Crash a node, then recover it", crash)

    def stop(ev):
        target = "eu-west-1b-n2"
        check(chaos("stop", "node", target)[0] == 200, "planned stop refused")
        wait_for(lambda: node(state(), target)["runtime"] == "stopped", 90, f"{target} to stop")
        exit_code = sh(CLI, "inspect", target, "--format", "{{.State.ExitCode}}").strip()
        ev["exit code"] = exit_code
        chaos("recover", "node", target)
        wait_for(lambda: node(state(), target)["liveness"] == "live", 180, f"{target} to come back")
        healthy()
    p.run("Planned stop (graceful drain), then recover", stop)

    def partition(ev):
        target = "eu-west-1a-n1"
        ip = node(state(), target)["ip"]
        chaos("partition", "node", target)
        wait_for(lambda: node(state(), target)["partitioned"], 30, f"{target} to be partitioned")
        chaos("recover", "node", target)
        s = wait_for(lambda: (lambda s: s if not node(s, target)["partitioned"] and node(s, target)["liveness"] == "live" else None)(state()),
                     180, f"{target} to rejoin")
        rules = tc(target)
        ev.update({"same address after heal": node(s, target)["ip"] == ip, "latency rules re-applied": "delay" in rules})
        check(node(s, target)["ip"] == ip and "delay" in rules, "heal lost the address or the latency rules")
        healthy()
    p.run("Partition a node off the network, then heal it (same address, latency restored)", partition)

    def brownout(ev):
        s = state()
        target = nodes_in(s, "us-east-1")[1]["name"]
        dst = nodes_in(s, "us-west-2")[0]["ip"]
        before = rtt(target, dst)
        chaos("brownout", "node", target, delay_ms=700)
        rules = tc(target)
        during = rtt(target, dst)
        chaos("recover", "node", target)
        after = rtt(target, dst)
        ev.update({"round trip before": f"{before} ms", "during brownout": f"{during} ms", "after recover": f"{after} ms"})
        check("delay 7" in rules, "no 700 ms delay rule while browned out")
        check(during and during > before + 500, "brownout didn't slow the network")
        check(after and after < before + 50, "recover didn't clear the brownout")
    p.run("Brownout adds ~700 ms to a node's network; recover clears it", brownout)

    def isolate(ev):
        s = state()
        eu = nodes_in(s, "eu-west-1")[0]["name"]
        east_ip, eu2_ip = nodes_in(s, "us-east-1")[0]["ip"], nodes_in(s, "eu-west-1")[1]["ip"]
        chaos("isolate", "region", "eu-west-1", confirm=True)
        rules = tc(eu)
        cross, local = rtt(eu, east_ip), rtt(eu, eu2_ip)
        ev.update({"rule": "loss 100% toward other regions" if "loss 100%" in rules else rules[-200:],
                   "eu-west-1 → us-east-1": "unreachable" if cross is None else f"{cross} ms",
                   "eu-west-1 → eu-west-1": f"{local} ms"})
        check("loss 100%" in rules and cross is None and local is not None, "isolation didn't cut only cross-region traffic")
        chaos("recover", "region", "eu-west-1")
        healthy()
    p.run("Isolate a region from the others while it stays reachable locally", isolate)

    def zone_outage(ev):
        chaos("outage", "zone", "us-west-2a", confirm=True)
        down = wait_for(lambda: [n["name"] for n in state()["nodes"] if n["zone"] == "us-west-2a" and n["liveness"] == "dead"],
                        90, "the zone's nodes to be down")
        ev["down"] = down
        recover_all()
        healthy()
    p.run("Zone outage takes down every node in the zone", zone_outage)

    def guard(ev):
        first = chaos("outage", "region", "eu-west-1")
        second = chaos("outage", "region", "us-west-2")
        ev.update({"first region outage": first[0], "second region outage": second[0],
                   "warning": second[1]["detail"].get("warning") if isinstance(second[1], dict) else second[1]})
        recover_all()
        healthy()
        check(first[0] == 200 and second[0] == 409 and second[1]["detail"]["needs_confirm"],
              "the guard didn't ask before a second region outage")
    p.run("Survival guard asks before exceeding the survival goal", guard)

    def supervisor(ev):
        target = "us-east-1b-n2"
        sh(CLI, "exec", target, "/cockroach/cockroach", "sql", "--insecure", f"--host={target}", "-e",
           "SET allow_unsafe_internals = true; SELECT crdb_internal.force_log_fatal('proof run: simulated fatal error')",
           check_rc=False, timeout=60)
        t0 = time.time()
        event = wait_for(lambda: next((e["text"] for e in state()["events"] if target in e["text"] and "restarted" in e["text"]), None),
                         90, "the supervisor to restart the node")
        wait_for(lambda: node(state(), target)["liveness"] == "live", 180, f"{target} to be live again")
        ev.update({"event": event, "restarted within": f"{time.time() - t0:.0f} s"})
        healthy()
    p.run("Supervisor restarts a node that stopped on a fatal error", supervisor)

    # ---- payments ----
    p.section = "Payments workload, retries and metrics"

    def payments(ev):
        check(api("POST", "/api/workload/start")[0] == 200, "payments refused to start")
        w = wait_for(lambda: (lambda w: w if w["tps"] > 50 and w["balance"] == 10_000_000 else None)(state()["workload"]),
                     90, "payments to run and the invariant to be checked")
        t = state()["traffic"]["cluster"]
        ev.update({"transfers/s": w["tps"], "total balance": f"${w['balance']:,}", "money lost": w["money_lost"],
                   "payments p50/p95/p99": f"{w['p50_ms']} / {w['p95_ms']} / {w['p99_ms']} ms",
                   "cluster p50/p95/p99": f"{t['p50_ms']} / {t['p95_ms']} / {t['p99_ms']} ms"})
        check(w["money_lost"] == 0 and t["p95_ms"] is not None, "payments or latency percentiles missing")
    p.run("Payments run with the $10,000,000 invariant and p50/p95/p99", payments)

    def outage_under_load(ev):
        sql("CREATE TABLE IF NOT EXISTS proof_probe (id UUID PRIMARY KEY DEFAULT gen_random_uuid())")
        time.sleep(12)  # baseline for recovery time
        before = state()["workload"]
        chaos("outage", "region", "us-east-1", confirm=True)
        commits, errors, deadline = 0, 0, time.time() + 30
        while time.time() < deadline:  # a plain client with timeouts and simple retries, like Banko's loader
            try:
                with psycopg.connect(host="localhost", port=26257, user="root", dbname="defaultdb", sslmode="disable",
                                     autocommit=True, connect_timeout=5, options="-c statement_timeout=5s") as conn:
                    while time.time() < deadline:
                        conn.execute("INSERT INTO proof_probe DEFAULT VALUES")
                        commits += 1
            except psycopg.Error:
                errors += 1
                time.sleep(0.5)
        w = wait_for(lambda: (lambda w: w if w["recovery_s"] is not None else None)(state()["workload"]), 150,
                     "payments to recover")
        recover_all()
        healthy()
        w = wait_for(lambda: (lambda w: w if w["last_check"] and w["last_check"] > time.time() - 8 else None)(state()["workload"]),
                     30, "a fresh invariant check")
        ev.update({"primary region down": "us-east-1", "recovery time": f"{w['recovery_s']:.1f} s",
                   "money lost": w["money_lost"], "failed transfers": w["failed_total"] - before["failed_total"],
                   "retries that found the transfer already applied": w["deduplicated"] - before["deduplicated"],
                   "transient errors retried": w["errors_total"], "plain-client commits during the outage": commits,
                   "plain-client errors it retried through": errors})
        sql("DROP TABLE IF EXISTS proof_probe")
        check(w["money_lost"] == 0 and w["failed_total"] == before["failed_total"] and commits > 20,
              "money was lost, transfers failed, or a plain client couldn't write")
    p.run("Primary region outage under load: $0 lost, nothing failed, recovery time measured", outage_under_load)
    api("POST", "/api/workload/stop")

    # ---- elasticity (+ scenarios while the primary has 3 nodes) ----
    p.section = "Elasticity"
    added = {}

    def add_node(ev):
        before = {n["name"] for n in state()["nodes"]}
        previous = op_id()
        check(api("POST", "/api/cluster/nodes", {"region": "us-east-1"})[0] == 202, "add node refused")
        check(wait_op(previous)["status"] == "done", "add node failed")
        s = healthy(7)
        added["node"] = next(n["name"] for n in s["nodes"] if n["name"] not in before)
        replicas = wait_for(lambda: node(state(), added["node"])["replicas"] or None, 240, "replicas to rebalance onto it")
        ev.update({"new node": added["node"], "replicas on it": replicas})
    p.run("Add a node: it joins and data rebalances onto it", add_node)

    def survival(ev):
        for goal in ("zone", "region"):
            previous = op_id()
            check(api("POST", "/api/cluster/survival", {"goal": goal})[0] == 202, f"{goal} survival refused")
            check(wait_op(previous)["status"] == "done", f"switch to {goal} failed")
            ev[goal] = sql("SELECT survival_goal FROM [SHOW SURVIVAL GOAL FROM DATABASE chaos]", one=True)[0]
        check(ev == {"zone": "zone", "region": "region"}, "survival goal didn't switch")
    p.run("Switch survival goal to ZONE and back to REGION", survival)

    def primary(ev):
        for region in ("us-west-2", "us-east-1"):
            previous = op_id()
            check(api("POST", "/api/cluster/primary", {"region": region})[0] == 202, "make primary refused")
            check(wait_op(previous)["status"] == "done", "make primary failed")
            ev[f"primary after moving to {region}"] = sql(
                'SELECT region FROM [SHOW REGIONS FROM DATABASE defaultdb] WHERE "primary"', one=True)[0]
        check(ev["primary after moving to us-west-2"] == "us-west-2" and ev["primary after moving to us-east-1"] == "us-east-1",
              "primary region didn't move")
    p.run("Move the primary region and back", primary)

    def regions(ev):
        previous = op_id()
        check(api("POST", "/api/cluster/regions", {"name": "ap-northeast-1", "provider": "aws", "nodes": 1})[0] == 202, "add region refused")
        check(wait_op(previous)["status"] == "done", "add region failed")
        listed = {r[0] for r in sql("SELECT region FROM [SHOW REGIONS FROM DATABASE defaultdb]")}
        ev["defaultdb regions after add"] = sorted(listed)
        check("ap-northeast-1" in listed, "new region missing from defaultdb")
        previous = op_id()
        check(api("DELETE", "/api/cluster/regions/ap-northeast-1")[0] == 202, "remove region refused")
        check(wait_op(previous)["status"] == "done", "remove region failed")
        ev["regions after remove"] = [r["name"] for r in healthy()["regions"]]
        check("ap-northeast-1" not in ev["regions after remove"], "region still there")
    p.run("Add a region to both databases, then remove it", regions)

    # ---- scenarios ----
    p.section = "Guided scenarios"
    for sc in api("GET", "/api/scenarios")[1]:
        def run_scenario(ev, sc=sc):
            check(not sc["unmet"], f"requirements not met: {sc['unmet']}")
            status, body = api("POST", f"/api/scenarios/{sc['id']}/start")
            check(status == 200, f"start refused: {body}")
            cur = lambda: api("GET", "/api/scenarios/current")[1]  # noqa: E731
            wait_for(lambda: not cur()["busy"], 900, "the scenario to prepare")
            for _ in range(sc["steps"]):
                api("POST", "/api/scenarios/next")
                wait_for(lambda: not cur()["busy"], 900, "the step to finish")
            steps = cur()["steps"]
            api("POST", "/api/scenarios/stop")
            for st in steps:
                ev[st["label"]] = st["error"] or st["result"]
            check(all(st["status"] == "done" for st in steps), "a step failed: " +
                  "; ".join(f"{st['label']}: {st['error']}" for st in steps if st["status"] != "done"))
            time.sleep(5)
            recover_all()
            api("POST", "/api/workload/stop")
            healthy()
        p.run(f"Scenario: {sc['title']}", run_scenario, long=True)

    p.section = "Elasticity"

    def decommission(ev):
        target = added.get("node")
        check(target, "no node was added earlier")
        previous = op_id()
        check(api("DELETE", f"/api/cluster/nodes/{target}")[0] == 202, "decommission refused")
        op = wait_op(previous)
        check(op["status"] == "done", f"decommission failed: {op.get('error')}")
        ev.update({"drained and removed": target, "steps": op["steps"][-4:]})
        healthy(6)
    p.run("Decommission a node: replicas drain first, then it's removed", decommission)

    # ---- booth and triggers ----
    p.section = "Booth mode and triggers"
    default_triggers = api("GET", "/api/settings")[1]

    def booth(ev):
        api("PUT", "/api/booth", {"enabled": True, "auto_heal_s": 10})
        api("POST", "/api/booth/counters/reset")
        status, fired = api("POST", "/api/triggers/roach-1")
        check(status == 200 and fired.get("ok"), f"trigger failed: {fired}")
        ev["trigger hit"] = f"{fired['action']} {fired['value']}"
        wait_for(lambda: node(state(), fired["value"])["liveness"] == "dead", 60, "the victim to go down")
        t0 = time.time()
        healthy()
        ev["auto-healed after"] = f"{time.time() - t0:.0f} s"
        ev["counters"] = state()["booth"]["counters"]
        check(ev["counters"]["nodes_crashed"] >= 1, "counters didn't count the hit")
    p.run("Booth: a trigger crashes a safe random node and everything auto-heals", booth)

    def booth_refuses(ev):
        api("PUT", "/api/triggers", {"triggers": {**default_triggers["triggers"],
                                                  "proof-region": {"action": "region-outage", "target": "region", "value": "us-west-2"}},
                                     "keys": default_triggers["keys"]})
        chaos("outage", "region", "eu-west-1", confirm=True)
        status, fired = api("POST", "/api/triggers/proof-region")
        ev["response"] = fired
        recover_all()
        healthy()
        check(status == 200 and fired.get("ok") is False and "Refused" in fired.get("message", ""),
              "booth mode didn't refuse an unsafe hit")
    p.run("Booth refuses hits that would make data unavailable", booth_refuses)

    def stage_keys(ev):
        result = browser(p, [{"do": "open", "url": STAGE}, {"do": "key", "key": "1"},
                             {"do": "waitFor", "as": "event", "timeout": 15000,
                              "expr": "[...document.querySelectorAll('#events .ev')].map(e => e.textContent).find(t => /crash/i.test(t)) || ''"}])
        ev["event on stage"] = result["event"]
        check(result["event"], "stage key 1 didn't fire the trigger")
        healthy()
    p.run("Stage key 1 fires the mapped trigger (USB button / Makey Makey path)", stage_keys)
    api("PUT", "/api/triggers", {"triggers": default_triggers["triggers"], "keys": default_triggers["keys"]})
    api("PUT", "/api/booth", {"enabled": False, "auto_heal_s": 45})

    p.section = "Security tokens"

    def tokens(ev):
        compose({"CONTROL_TOKEN": "proof-control", "TRIGGER_TOKEN": "proof-trigger", "DEPLOYMENT_MODEL": "multi-region"})
        try:
            ev["change without token"] = api("POST", "/api/cluster/latency", {"setting": "realistic"})[0]
            ev["trigger without token"] = api("POST", "/api/triggers/big-red")[0]
            ev["trigger with X-Trigger-Token"] = api("POST", "/api/triggers/big-red", headers={"X-Trigger-Token": "proof-trigger"})[0]
            jar = http.cookiejar.CookieJar()
            opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))
            opener.open(f"{PANEL}/presenter?token=proof-control", timeout=30).read()
            ev["change after /presenter?token=…"] = api("POST", "/api/cluster/latency", {"setting": "realistic"}, opener=opener)[0]
            ev["stage still readable"] = api("GET", "/api/state")[0]
        finally:
            compose({"CONTROL_TOKEN": "", "TRIGGER_TOKEN": "", "DEPLOYMENT_MODEL": "multi-region"})
        check(ev["change without token"] == 401 and ev["trigger without token"] == 401 and
              ev["trigger with X-Trigger-Token"] == 200 and ev["change after /presenter?token=…"] == 200 and
              ev["stage still readable"] == 200, "token checks failed")
    p.run("CONTROL_TOKEN and TRIGGER_TOKEN lock changes; the stage stays readable", tokens, long=True)

    # ---- UI ----
    p.section = "Stage and presenter UI"

    def stage_ui(ev):
        r = browser(p, [
            {"do": "open", "url": STAGE + "?theme=dark"},
            {"do": "eval", "as": "regions", "expr": "document.querySelectorAll('#regions .region').length"},
            {"do": "eval", "as": "tiles", "expr": "document.querySelectorAll('#regions .node').length"},
            {"do": "eval", "as": "tooltips", "expr": "document.querySelectorAll('[data-tip]').length"},
            {"do": "eval", "as": "text", "expr": "document.body.innerText"},
            {"do": "shot", "file": p.shot("stage-dark")},
            {"do": "key", "key": "t"}, {"do": "eval", "as": "theme_after_t", "expr": "document.documentElement.dataset.theme"},
            {"do": "shot", "file": p.shot("stage-light")}, {"do": "key", "key": "t"},
            {"do": "hover", "selector": "#k-money", "settle": 900},
            {"do": "eval", "as": "money_tip", "expr": "(document.querySelector('.tooltip:not([hidden])') || {}).textContent || ''"},
            {"do": "shot", "file": p.shot("stage-tooltip")}, {"do": "move", "x": 5, "y": 5},
            {"do": "key", "key": "p", "settle": 1500},
            {"do": "eval", "as": "drawer_open", "expr": "!document.getElementById('drawer').hidden"},
            {"do": "key", "key": "Escape"},
            {"do": "eval", "as": "drawer_closed", "expr": "document.getElementById('drawer').hidden"},
            {"do": "click", "selector": "#regions .node"},
            {"do": "eval", "as": "menu_items", "expr": "document.getElementById('menu').hidden ? 0 : document.querySelectorAll('#menu button').length"},
            {"do": "shot", "file": p.shot("stage-node-menu")}, {"do": "key", "key": "Escape"},
        ])
        bad = [w for w in FORBIDDEN if w in r["text"].lower()]
        ev.update({"region cards": r["regions"], "node tiles": r["tiles"], "tooltips on screen": r["tooltips"],
                   "T switches theme": r["theme_after_t"], "Money lost tooltip": r["money_tip"][:110] + "…",
                   "P opens the drawer / Esc closes it": f"{r['drawer_open']} / {r['drawer_closed']}",
                   "node menu items": r["menu_items"], "cloud-illusion words on stage": bad or "none",
                   "dark": p.shot("stage-dark"), "light": p.shot("stage-light"), "tooltip": p.shot("stage-tooltip"),
                   "node menu": p.shot("stage-node-menu")})
        expected_tiles = state()["cluster"]["total_nodes"]
        check(r["regions"] == 3 and r["tiles"] == expected_tiles and r["tooltips"] > 20,
              f"stage didn't render the cluster ({r['tiles']} tiles for {expected_tiles} nodes)")
        check(r["theme_after_t"] == "light" and r["money_tip"].startswith("Money lost"), "theme or tooltip broken")
        check(r["drawer_open"] and r["drawer_closed"] and r["menu_items"] >= 5, "drawer or node menu broken")
        check(not bad, f"stage shows {bad}")
    p.run("Stage renders, themes, tooltips, drawer, node menu, no infrastructure words", stage_ui)

    def feedback_and_confirm(ev):
        chaos("outage", "region", "eu-west-1")
        time.sleep(3)
        r = browser(p, [
            {"do": "open", "url": STAGE},
            {"do": "click", "selector": "#regions .region:nth-child(2) .ractions .btn.danger", "settle": 1500},
            {"do": "eval", "as": "confirm", "expr": "!document.getElementById('confirm').hidden && document.getElementById('confirm-text').textContent"},
            {"do": "shot", "file": p.shot("stage-confirm")},
            {"do": "click", "selector": "#confirm-cancel"},
            {"do": "click", "selector": "#regions .region:nth-child(1) .ractions .btn:nth-child(4)", "settle": 50},
            {"do": "waitFor", "as": "feedback", "timeout": 8000,
             "expr": "document.querySelector('.ractions .btn.working, .ractions .btn.done') ? 'working/done shown' : ''"},
        ])
        recover_all()
        healthy()
        ev.update({"confirm dialog": r["confirm"], "button feedback": r["feedback"], "screenshot": p.shot("stage-confirm")})
        check(r["confirm"] and r["feedback"], "confirm dialog or button feedback missing")
    p.run("Guard confirm dialog and button feedback on the stage", feedback_and_confirm)

    def confetti(ev):
        target = "us-west-2a-n1"
        path = p.out / "plan-confetti.json"
        path.write_text(json.dumps([
            {"do": "open", "url": STAGE},
            {"do": "waitFor", "as": "celebrated", "timeout": 150000,
             "expr": "!document.getElementById('celebrate').hidden && document.getElementById('celebrate-text').textContent"},
            {"do": "wait", "ms": 600}, {"do": "shot", "file": p.shot("stage-confetti")}]), encoding="utf-8")
        proc = subprocess.Popen(["node", str(ROOT / "tools" / "proof-browser.mjs"), str(path)], stdout=subprocess.PIPE, text=True)
        time.sleep(6)  # the stage is open and watching
        chaos("crash", "node", target)
        wait_for(lambda: node(state(), target)["liveness"] == "dead", 60, "the crash to register")
        time.sleep(4)
        chaos("recover", "node", target)
        out_, _ = proc.communicate(timeout=240)
        r = json.loads(out_.strip().splitlines()[-1])
        ev.update({"banner": r.get("celebrated"), "screenshot": p.shot("stage-confetti")})
        check(r.get("celebrated"), f"no celebration: {r.get('error')}")
        healthy()
    p.run("Recovery celebration (confetti) after a node comes back", confetti)

    def presenter_ui(ev):
        r = browser(p, [
            {"do": "open", "url": f"{PANEL}/presenter", "width": 1100, "height": 900},
            {"do": "eval", "as": "tabs", "expr": "document.querySelectorAll('.tabs button').length"},
            {"do": "eval", "as": "summary", "expr": "document.getElementById('cluster-now').innerText"},
            {"do": "eval", "as": "links", "expr": "[...document.querySelectorAll('a.quick')].map(a => a.target + ' ' + a.textContent)"},
            {"do": "shot", "file": p.shot("presenter-cluster")},
            {"do": "click", "selector": "[data-tab=chaos]"}, {"do": "shot", "file": p.shot("presenter-chaos")},
            {"do": "click", "selector": "[data-tab=scenarios]"}, {"do": "wait", "ms": 800},
            {"do": "eval", "as": "scenarios", "expr": "document.querySelectorAll('#sc-list .sc').length"},
            {"do": "click", "selector": "[data-tab=cluster]"},
        ])
        phone = browser(p, [
            {"do": "open", "url": f"{PANEL}/presenter", "width": 390, "height": 844, "mobile": True},
            {"do": "eval", "as": "overflow", "expr": "document.scrollingElement.scrollWidth - window.innerWidth"},
            {"do": "shot", "file": p.shot("presenter-phone")},
        ])
        ev.update({"tabs": r["tabs"], "scenarios listed": r["scenarios"], "quick links": r["links"],
                   "phone horizontal overflow (px)": phone["overflow"],
                   "cluster tab": p.shot("presenter-cluster"), "chaos tab": p.shot("presenter-chaos"), "phone": p.shot("presenter-phone")})
        check(r["tabs"] == 6 and r["scenarios"] >= 12 and "multi-region" in r["summary"], "presenter incomplete")
        check(all(l.startswith("_blank") for l in r["links"]) and len(r["links"]) == 3, "quick links don't open in a new tab")
        check(phone["overflow"] <= 2, "presenter overflows on a phone")
    p.run("Presenter console: tabs, model, new-tab links, phone layout", presenter_ui)

    # ---- explainers ----
    p.section = "Explainers"

    def explainers(ev):
        listed = [e["name"] for e in api("GET", "/api/explainers")[1]]
        page = api("GET", "/explainer/chaos")[1]
        from controlplane.explainers import render_chaos
        azure = load_preset(ROOT / "presets", "azure", catalog, default_version="v26.3.2", subnet="10.77.0.0/16", max_nodes=15)
        html_ = render_chaos((ROOT / "web" / "explainers" / "chaos.html").read_text(), azure, catalog, "Contoso Pay")
        r = browser(p, [{"do": "open", "url": f"{PANEL}/explainer/chaos?capture=1&t=20", "settle": 4000},
                        {"do": "shot", "file": p.shot("explainer-chaos")}])
        ev.update({"explainers offered": listed, "names the running cluster": "Global Payments" in page and "us-east-1" in page,
                   "azure preset gets azure regions": "eastus" in html_ and "us-east-1" not in html_.split("<script>")[1][:20000],
                   "frame": p.shot("explainer-chaos")})
        check("chaos" in listed and ev["names the running cluster"] and ev["azure preset gets azure regions"], "explainer checks failed")
        check(r is not None, "explainer didn't render")
    p.run("Chaos explainer follows the running preset (and other clouds)", explainers)

    def exporter(ev):
        if not shutil.which("ffmpeg"):
            ev["ffmpeg"] = "not installed: skipped"
            return
        target = p.out / "explainer-smoke.mp4"
        sh("node", "tools/export-explainer.mjs", "--name", "chaos", "--duration", "2", "--out", str(target), timeout=300)
        frames = sh("ffprobe", "-v", "error", "-count_frames", "-select_streams", "v:0", "-show_entries",
                    "stream=width,height,nb_read_frames", "-of", "csv=p=0", str(target)).strip()
        ev.update({"video": target.name, "width,height,frames": frames})
        check(frames.startswith("1920,1080,60"), f"unexpected video: {frames}")
    p.run("Explainer exports to a 1080p MP4", exporter, long=True)

    # ---- single region ----
    p.section = "Single-region deployment"

    def single(ev):
        s = rebuild("aws", "single-region", expect_nodes=3)
        c = s["cluster"]
        rules = tc(s["nodes"][0]["name"])
        listed = [e["name"] for e in api("GET", "/api/explainers")[1]]
        r = browser(p, [{"do": "open", "url": STAGE},
                        {"do": "eval", "as": "buttons", "expr": "[...document.querySelectorAll('.ractions .btn')].map(b => b.textContent)"},
                        {"do": "eval", "as": "footprint", "expr": "document.getElementById('footprint').textContent"},
                        {"do": "shot", "file": p.shot("single-region-stage")}])
        ev.update({"regions": [r_["name"] for r_ in s["regions"]], "zones": sorted({n["zone"] for n in s["nodes"]}),
                   "survival goal": c["survival_goal"], "model": c["model"], "stage buttons": r["buttons"],
                   "header": r["footprint"], "latency rules": "none (single region)" if "netem" not in rules else rules[-160:],
                   "explainers offered": listed, "stage": p.shot("single-region-stage")})
        check(c["model"] == "single-region" and len(s["regions"]) == 1 and c["survival_goal"] == "zone", "not a single-region cluster")
        check(len({n["zone"] for n in s["nodes"]}) == 3, "nodes aren't spread over 3 zones")
        check(sum(b.startswith("Outage · ") for b in r["buttons"]) == 3, "no per-zone outage buttons")
        check("netem" not in rules and "chaos" not in listed, "shaping or explainer not adapted to one region")
    p.run("Single region: 3 nodes across 3 zones, ZONE survival, zone buttons", single)

    def survive_zone(ev):
        status, body = api("POST", "/api/scenarios/survive-zone/start")
        check(status == 200, f"start refused: {body}")
        cur = lambda: api("GET", "/api/scenarios/current")[1]  # noqa: E731
        wait_for(lambda: not cur()["busy"], 300, "the scenario to prepare")
        for _ in range(4):
            api("POST", "/api/scenarios/next")
            wait_for(lambda: not cur()["busy"], 600, "the step to finish")
        steps = cur()["steps"]
        api("POST", "/api/scenarios/stop")
        for st in steps:
            ev[st["label"]] = st["error"] or st["result"]
        time.sleep(5)
        recover_all()
        api("POST", "/api/workload/stop")
        healthy()
        check(all(st["status"] == "done" for st in steps), "a step failed")
        impact = steps[2]["result"] or {}
        check(impact.get("Money lost") == "$0", f"money lost: {impact}")
    p.run("Single region: payments survive a zone outage with $0 lost", survive_zone)

    # ---- custom region names ----
    p.section = "Custom presets and region names"

    def custom(ev):
        yaml_text = ("display_name: Proof Mars Bank\ncluster_id: proof-mars\nregions:\n"
                     "  - { name: mars-north-1, provider: custom, nodes: 1, primary: true, label: Olympus Mons, location: [18.6, -133.8] }\n"
                     "  - { name: mars-south-1, provider: custom, nodes: 1, label: Hellas Basin, location: [-42.4, 70.5] }\n"
                     "  - { name: phobos-1, provider: custom, nodes: 1, label: Stickney, location: [1.0, -49.0] }\n")
        status, body = api("PUT", "/api/presets/custom/proof-mars", {"yaml": yaml_text})
        check(status == 200, f"custom preset refused: {body}")
        try:
            s = rebuild("proof-mars", "multi-region", expect_nodes=3)
            r = browser(p, [{"do": "open", "url": STAGE}, {"do": "shot", "file": p.shot("custom-regions-stage")},
                            {"do": "eval", "as": "title", "expr": "document.getElementById('cluster-name').textContent"}])
            ev.update({"regions": [x["name"] for x in s["regions"]], "nodes": [x["name"] for x in s["nodes"]],
                       "stage title": r["title"], "stage": p.shot("custom-regions-stage")})
            check([x["name"] for x in s["regions"]] == ["mars-north-1", "mars-south-1", "phobos-1"], "region names not used")
            check(r["title"] == "Proof Mars Bank", "display name not on stage")
        finally:
            (ROOT / "presets" / "custom" / "proof-mars.yaml").unlink(missing_ok=True)
    p.run("Any region names: a custom preset builds and shows on stage", custom, long=True)

    # ---- restore ----
    p.section = "Back to the default"

    def restore(ev):
        s = rebuild("aws", "multi-region", expect_nodes=6)
        api("POST", "/api/workload/stop")
        api("POST", "/api/cluster/latency", {"setting": "realistic"})
        ev.update({"cluster": f"{s['cluster']['preset']} {s['cluster']['model']} {s['cluster']['live_nodes']}/{s['cluster']['total_nodes']}"})
    p.run("Cluster restored to the default (aws, multi-region, 6 nodes)", restore)

    summary = {k: sum(r["status"] == k for r in p.results) for k in ("pass", "fail", "skip")}
    print(f"\n{summary['pass']} passed, {summary['fail']} failed, {summary['skip']} skipped in "
          f"{(time.time() - p.started) / 60:.0f} min. Report: {out / 'index.html'}", flush=True)
    return 1 if summary["fail"] else 0


if __name__ == "__main__":
    sys.exit(main())
