
import os
import asyncio
import random
import subprocess
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import requests
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

EAST_API = os.getenv("EAST_API", "http://toxiproxy-east:8474")
WEST_API = os.getenv("WEST_API", "http://toxiproxy-west:8474")
CENTRAL_API = os.getenv("CENTRAL_API", "http://toxiproxy-central:8474")

EAST_PROXIES = os.getenv("EAST_PROXIES","e1a,e1b").split(",")
WEST_PROXIES = os.getenv("WEST_PROXIES","w2a,w2b").split(",")
CENTRAL_PROXIES = os.getenv("CENTRAL_PROXIES","c1").split(",")

REGIONS = {
    "us-east-1": {"api": EAST_API, "proxies": EAST_PROXIES, "color": "#16a34a", "containers": ["crdb-e1a", "crdb-e1b"]},
    "us-west-2": {"api": WEST_API, "proxies": WEST_PROXIES, "color": "#2563eb", "containers": ["crdb-w2a", "crdb-w2b"]},
    "us-central-1": {"api": CENTRAL_API, "proxies": CENTRAL_PROXIES, "color": "#f59e0b", "containers": ["crdb-c1"]}
}

DB_HOST = os.getenv("DB_HOST", "haproxy")
DB_PORT = os.getenv("DB_PORT", "26257")
DB_USER = os.getenv("DB_USER", "root")
DB_NAME = os.getenv("DB_NAME", "defaultdb")

transaction_count = 0

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse("static/index.html")

def _list_proxies(api):
    r = requests.get(f"{api}/proxies", timeout=3)
    r.raise_for_status()
    data = r.json()
    # Toxiproxy returns a dict, not an array
    if isinstance(data, dict):
        return data
    return {p["name"]: p for p in data}

def _set_enabled(api, name, enabled: bool):
    r = requests.post(f"{api}/proxies/{name}", json={"enabled": enabled}, timeout=3)
    if r.status_code == 404:
        raise HTTPException(404, f"Proxy {name} not found at {api}")
    r.raise_for_status()

def _add_latency(api, name, ms):
    tname = "latency"
    requests.post(f"{api}/proxies/{name}/toxics", json={
        "name": tname, "type": "latency", "stream": "downstream",
        "attributes": {"latency": int(ms), "jitter": int(ms/3)}
    }, timeout=3)

def _clear_toxics(api, name):
    r = requests.get(f"{api}/proxies/{name}/toxics", timeout=3)
    if r.status_code == 200:
        for toxic in r.json():
            requests.delete(f"{api}/proxies/{name}/toxics/{toxic['name']}", timeout=3)

def _check_containers_running(containers):
    """Check if all containers in the list are running"""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "status=running", "--format", "{{.Names}}"],
            capture_output=True,
            timeout=5,
            text=True
        )
        if result.returncode == 0:
            running_containers = set(result.stdout.strip().split('\n'))
            return all(c in running_containers for c in containers)
    except Exception:
        pass
    return True  # Default to true if we can't check (fail open)

@app.get("/api/status")
def status():
    result = {}
    for region, cfg in REGIONS.items():
        try:
            proxies = _list_proxies(cfg["api"])
            proxies_enabled = any(proxies.get(n, {}).get("enabled", False) for n in cfg["proxies"])
            containers_running = _check_containers_running(cfg["containers"])
            
            # Region is up only if BOTH proxies are enabled AND containers are running
            up = proxies_enabled and containers_running
            
            result[region] = {
                "up": up,
                "proxies": {n: proxies.get(n, {}) for n in cfg["proxies"]},
                "containers_running": containers_running
            }
        except Exception as e:
            result[region] = {"up": False, "error": str(e)}
    return result

@app.post("/api/kill/{region}")
def kill_region(region: str):
    if region not in REGIONS: raise HTTPException(404, "Unknown region")
    cfg = REGIONS[region]
    for name in cfg["proxies"]:
        _clear_toxics(cfg["api"], name)
        _set_enabled(cfg["api"], name, False)
    return {"ok": True, "region": region, "action": "kill"}

@app.post("/api/recover/{region}")
def recover_region(region: str):
    if region not in REGIONS: raise HTTPException(404, "Unknown region")
    cfg = REGIONS[region]
    for name in cfg["proxies"]:
        _clear_toxics(cfg["api"], name)
        _set_enabled(cfg["api"], name, True)
    return {"ok": True, "region": region, "action": "recover"}

@app.post("/api/brownout/{region}")
def brownout_region(region: str, ms: int = 700):
    if region not in REGIONS: raise HTTPException(404, "Unknown region")
    cfg = REGIONS[region]
    for name in cfg["proxies"]:
        _clear_toxics(cfg["api"], name)
        _set_enabled(cfg["api"], name, True)
        _add_latency(cfg["api"], name, ms)
    return {"ok": True, "region": region, "action": "brownout", "latency_ms": ms}

@app.post("/api/kill-nodes/{region}")
def kill_nodes(region: str):
    """Actually stop CockroachDB containers (node failure, not network partition)"""
    if region not in REGIONS: raise HTTPException(404, "Unknown region")
    cfg = REGIONS[region]
    stopped = []
    for container in cfg["containers"]:
        try:
            result = subprocess.run(["docker", "stop", container], capture_output=True, timeout=10)
            if result.returncode == 0:
                stopped.append(container)
        except Exception as e:
            pass
    return {"ok": True, "region": region, "action": "kill_nodes", "stopped": stopped}

@app.post("/api/recover-nodes/{region}")
def recover_nodes(region: str):
    """Restart CockroachDB containers and enable proxies"""
    if region not in REGIONS: raise HTTPException(404, "Unknown region")
    cfg = REGIONS[region]
    started = []
    for container in cfg["containers"]:
        try:
            result = subprocess.run(["docker", "start", container], capture_output=True, timeout=10)
            if result.returncode == 0:
                started.append(container)
        except Exception as e:
            pass
    # Also enable proxies
    for name in cfg["proxies"]:
        _clear_toxics(cfg["api"], name)
        _set_enabled(cfg["api"], name, True)
    return {"ok": True, "region": region, "action": "recover_nodes", "started": started}

def get_db_conn():
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            database=DB_NAME,
            connect_timeout=3
        )
    except Exception as e:
        return None

@app.get("/api/cluster-health")
def cluster_health():
    try:
        conn = get_db_conn()
        if not conn:
            return {"error": "Cannot connect to cluster", "nodes": 0, "ranges": 0}
        
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT count(DISTINCT node_id) as node_count FROM crdb_internal.gossip_liveness WHERE decommissioning = false")
            nodes = cur.fetchone()
            
            cur.execute("SELECT count(DISTINCT range_id) as range_count FROM crdb_internal.ranges_no_leases")
            ranges = cur.fetchone()
            
            cur.execute("SELECT count(*) as replicas_count FROM crdb_internal.ranges_no_leases")
            replicas = cur.fetchone()
            
        conn.close()
        return {
            "nodes": nodes['node_count'] if nodes else 0,
            "ranges": ranges['range_count'] if ranges else 0,
            "replicas": replicas['replicas_count'] if replicas else 0,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {"error": str(e), "nodes": 0, "ranges": 0}

@app.get("/api/transactions")
def get_transactions():
    global transaction_count
    return {"count": transaction_count, "timestamp": datetime.utcnow().isoformat()}

@app.post("/api/simulate-writes")
async def simulate_writes(count: int = 10):
    global transaction_count
    success = 0
    failed = 0
    
    for i in range(count):
        try:
            conn = get_db_conn()
            if conn:
                with conn.cursor() as cur:
                    cur.execute(f"INSERT INTO defaultdb.demo_transactions (ts, amount) VALUES (now(), {random.randint(1, 1000)}) ON CONFLICT DO NOTHING")
                conn.commit()
                conn.close()
                success += 1
                transaction_count += 1
            else:
                failed += 1
        except Exception as e:
            failed += 1
        await asyncio.sleep(0.01)
    
    return {"success": success, "failed": failed, "total_count": transaction_count}
