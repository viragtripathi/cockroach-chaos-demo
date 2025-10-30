
import os
import asyncio
import random
import subprocess
from datetime import datetime
from fastapi import FastAPI, HTTPException, Request
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

def _get_docker_network():
    """Dynamically detect the Docker Compose network name"""
    try:
        # Try common network name patterns
        for pattern in ["cockroach-chaos-demo_default", "default"]:
            result = subprocess.run(
                ["docker", "network", "ls", "--filter", f"name={pattern}", "--format", "{{.Name}}"],
                capture_output=True,
                timeout=3,
                text=True
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip().split('\n')[0]
        
        # Fallback: inspect one of our containers to get its network
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{range $key, $value := .NetworkSettings.Networks}}{{$key}}{{end}}", "crdb-e1a"],
            capture_output=True,
            timeout=3,
            text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip().split('\n')[0]
    except Exception:
        pass
    
    # Default fallback
    return "cockroach-chaos-demo_default"

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse("static/index.html")

@app.get("/api/config")
def get_config(request: Request):
    """Return dynamic configuration based on the request host"""
    host = request.headers.get("host", "localhost:8088")
    # Extract hostname without port
    hostname = host.split(":")[0]
    
    return {
        "haproxy_stats_url": f"http://{hostname}:8404/stats",
        "crdb_admin_url": f"http://{hostname}:8080",
        "db_connection_string": f"postgresql://root@{hostname}:26257/defaultdb?sslmode=disable",
        "hostname": hostname
    }

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

@app.post("/api/partition/{region}")
def partition_region(region: str):
    """Simulate network partition by disconnecting containers from bridge network"""
    if region not in REGIONS: raise HTTPException(404, "Unknown region")
    cfg = REGIONS[region]
    disconnected = []
    network_name = _get_docker_network()
    
    for container in cfg["containers"]:
        try:
            result = subprocess.run(
                ["docker", "network", "disconnect", network_name, container],
                capture_output=True, 
                timeout=5
            )
            if result.returncode == 0:
                disconnected.append(container)
        except Exception as e:
            pass
    
    # Also disable toxiproxy to block external access
    for name in cfg["proxies"]:
        _clear_toxics(cfg["api"], name)
        _set_enabled(cfg["api"], name, False)
    
    return {"ok": True, "region": region, "action": "partition", "disconnected": disconnected}

@app.post("/api/recover/{region}")
def recover_region(region: str):
    """Recover from network partition or node failure"""
    if region not in REGIONS: raise HTTPException(404, "Unknown region")
    cfg = REGIONS[region]
    
    # Reconnect to network
    network_name = _get_docker_network()
    reconnected = []
    for container in cfg["containers"]:
        try:
            # Try to reconnect (will fail if already connected, which is fine)
            subprocess.run(
                ["docker", "network", "connect", network_name, container],
                capture_output=True,
                timeout=5
            )
            reconnected.append(container)
        except Exception:
            pass
    
    # Restart containers if they're not running
    started = []
    for container in cfg["containers"]:
        try:
            # Check if container is running
            check = subprocess.run(
                ["docker", "inspect", "-f", "{{.State.Running}}", container],
                capture_output=True,
                timeout=5,
                text=True
            )
            if check.returncode == 0 and check.stdout.strip() == "false":
                result = subprocess.run(["docker", "start", container], capture_output=True, timeout=10)
                if result.returncode == 0:
                    started.append(container)
        except Exception:
            pass
    
    # Enable proxies
    for name in cfg["proxies"]:
        _clear_toxics(cfg["api"], name)
        _set_enabled(cfg["api"], name, True)
    
    return {"ok": True, "region": region, "action": "recover", "reconnected": reconnected, "started": started}

@app.post("/api/brownout/{region}")
def brownout_region(region: str, ms: int = 700):
    """Simulate degraded network performance with latency"""
    if region not in REGIONS: raise HTTPException(404, "Unknown region")
    cfg = REGIONS[region]
    
    for name in cfg["proxies"]:
        _clear_toxics(cfg["api"], name)
        _set_enabled(cfg["api"], name, True)
        _add_latency(cfg["api"], name, ms)
    
    return {"ok": True, "region": region, "action": "brownout", "latency_ms": ms}

@app.post("/api/kill/{region}")
def kill_nodes(region: str):
    """Abrupt node failure using docker kill (SIGKILL) - simulates crash"""
    if region not in REGIONS: raise HTTPException(404, "Unknown region")
    cfg = REGIONS[region]
    killed = []
    
    for container in cfg["containers"]:
        try:
            # Use docker kill with SIGKILL (like kill -9) for abrupt failure
            result = subprocess.run(
                ["docker", "kill", "-s", "SIGKILL", container],
                capture_output=True,
                timeout=10
            )
            if result.returncode == 0:
                killed.append(container)
        except Exception as e:
            pass
    
    # Also disable toxiproxy to block external access
    for name in cfg["proxies"]:
        _clear_toxics(cfg["api"], name)
        _set_enabled(cfg["api"], name, False)
    
    return {"ok": True, "region": region, "action": "kill", "killed": killed}

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
