#!/usr/bin/env bash
# Usage: ./run.sh up [--preset NAME] [--model multi-region|single-region] | down [--legacy] | reset [same as up] | status | prefetch | test [pytest args] | prove [--quick]
set -euo pipefail
cd "$(dirname "$0")"

if [ -f .env ]; then set -a; . ./.env; set +a; fi
PANEL_PORT="${PANEL_PORT:-8088}"
CHAOS_SUBNET="${CHAOS_SUBNET:-10.77.0.0/16}"
CRDB_VERSION="${CRDB_VERSION:-v26.3.2}"
DEPLOYMENT_MODEL="${DEPLOYMENT_MODEL:-multi-region}"
export PANEL_PORT CHAOS_SUBNET CRDB_VERSION DEPLOYMENT_MODEL

if command -v docker >/dev/null 2>&1; then DOCKER=docker
elif command -v podman >/dev/null 2>&1; then DOCKER=podman
else echo "Neither docker nor podman found. Install one first." >&2; exit 1; fi
if $DOCKER compose version >/dev/null 2>&1; then DC="$DOCKER compose"
elif command -v docker-compose >/dev/null 2>&1; then DC="docker-compose"
else echo "docker compose is not available." >&2; exit 1; fi

PANEL="http://localhost:${PANEL_PORT}"
LEGACY_PROJECT="cockroach-chaos-demo"
LEGACY_CONTAINERS="crdb-e1a crdb-e1b crdb-w2a crdb-w2b crdb-c1 crdb-init crdb-bootstrap toxiproxy-east toxiproxy-west toxiproxy-central chaos-backend"

# Prints "operation-id|operation-status|cluster-status|preset|model|operation-error" from /api/state.
read -r -d '' STATE_PY <<'PY' || true
import json, sys
d = json.load(sys.stdin)
op = d.get("operation") or {}
c = d.get("cluster") or {}
print(op.get("id", ""), op.get("status", ""), c.get("status", ""), c.get("preset", ""), c.get("model", ""),
      (op.get("error") or "").replace("\n", " "), sep="|")
PY

read -r -d '' STATUS_PY <<'PY' || true
import json, sys
d = json.load(sys.stdin)
c = d.get("cluster")
if not c:
    print("No cluster yet.")
    sys.exit()
print(f"{c['display_name']} ({c['preset']}, {c.get('model', 'multi-region')}): {c['status']}, {c['live_nodes']}/{c['total_nodes']} nodes live, survival goal {c['survival_goal']}")
for r in d["regions"]:
    print(f"  {r['name']:<16} {r['state']:<9} {len(r['nodes'])} nodes")
op = d.get("operation") or {}
if op.get("status") == "failed":
    print(f"Last operation failed: {op.get('error')}")
PY

usage() { sed -n '2p' "$0" | sed 's/^# //'; exit 1; }

preset_file() {
  if [ -f "presets/$1.yaml" ]; then echo "presets/$1.yaml"
  elif [ -f "presets/custom/$1.yaml" ]; then echo "presets/custom/$1.yaml"
  else
    echo "Unknown preset '$1'. Available: $(cd presets && ls *.yaml | sed 's/\.yaml$//' | tr '\n' ' ')" >&2
    exit 1
  fi
}

cluster_state() { curl -fsS "$PANEL/api/state" 2>/dev/null | python3 -c "$STATE_PY" 2>/dev/null || echo "|||||"; }

request_rebuild() {
  local out code
  out="$(curl -sS -X POST "$PANEL/api/cluster/rebuild" -H 'Content-Type: application/json' -d "{\"preset\":\"$1\",\"model\":\"$2\"}" -w $'\n%{http_code}')" || true
  code="${out##*$'\n'}"
  if [ "$code" != "202" ]; then echo; echo "Rebuild refused (HTTP $code): ${out%$'\n'*}" >&2; exit 1; fi
}

wait_ready() {
  local want="$1" want_model="${2:-$DEPLOYMENT_MODEL}" asked=0 stale="" id op cl preset model err
  printf 'Waiting for the cluster'
  for _ in $(seq 1 1800); do  # 30 minutes: a first image download on booth Wi-Fi can be slow
    IFS='|' read -r id op cl preset model err <<<"$(cluster_state)"
    if [ "$op" = "failed" ] && [ "$id" != "$stale" ]; then
      if [ "$asked" = 0 ]; then
        echo; echo "The last attempt failed ($err); retrying as '$want' ($want_model)."
        request_rebuild "$want" "$want_model"; asked=1; stale="$id"
      else
        echo; echo "Provisioning failed: $err" >&2; exit 1
      fi
    elif [ -n "$preset" ] && { [ "$preset" != "$want" ] || { [ -n "$model" ] && [ "$model" != "$want_model" ]; }; } \
         && [ "$op" != "running" ] && [ "$asked" = 0 ]; then
      echo; echo "The running cluster is '$preset' (${model:-multi-region}); rebuilding it as '$want' ($want_model). This wipes cluster data."
      request_rebuild "$want" "$want_model"; asked=1; stale="$id"
    elif [ "$preset" = "$want" ] && { [ -z "$model" ] || [ "$model" = "$want_model" ]; } && [ "$op" = "done" ] && [ "$cl" = "healthy" ]; then
      echo; echo "Ready: preset '$want' ($want_model)."
      echo "  Stage:       $PANEL"
      echo "  Presenter:   $PANEL/presenter"
      echo "  SQL (Banko): postgresql://root@localhost:26257/defaultdb?sslmode=disable"
      echo "  DB Console:  http://localhost:8080"
      return 0
    fi
    printf '.'
    sleep 1
  done
  echo; echo "Still not ready after 30 minutes. See ./run.sh status and: $DC logs control-plane" >&2; exit 1
}

cmd_up() {
  local preset="${PRESET:-aws}" model="$DEPLOYMENT_MODEL"
  while [ $# -gt 0 ]; do
    case "$1" in
      --preset) preset="${2:?--preset needs a name}"; shift 2;;
      --model) model="${2:?--model needs multi-region or single-region}"; shift 2;;
      --single-region) model=single-region; shift;;
      --multi-region) model=multi-region; shift;;
      *) usage;;
    esac
  done
  preset="$(printf '%s' "$preset" | tr '[:upper:]' '[:lower:]')"
  case "$model" in multi-region|single-region) ;; *) echo "Unknown deployment model '$model': use multi-region or single-region." >&2; exit 1;; esac
  local file; file="$(preset_file "$preset")"
  echo "Preflight:"
  python3 tools/preflight.py --preset-file "$file" --subnet "$CHAOS_SUBNET" --panel-port "$PANEL_PORT" --model "$model"
  PRESET="$preset" DEPLOYMENT_MODEL="$model" $DC up -d --build
  wait_ready "$preset" "$model"
}

cmd_down() {
  if [ "${1:-}" = "--legacy" ]; then
    echo "Removing the pre-redesign demo..."
    $DOCKER rm -f $LEGACY_CONTAINERS >/dev/null 2>&1 || true
    if [ "$($DOCKER inspect -f '{{ index .Config.Labels "com.docker.compose.project" }}' haproxy 2>/dev/null || true)" = "$LEGACY_PROJECT" ]; then
      $DOCKER rm -f haproxy >/dev/null
    fi
    $DOCKER network rm "${LEGACY_PROJECT}_default" >/dev/null 2>&1 || true
    echo "Done. Start the new stack with: ./run.sh up"
    return
  fi
  local ids; ids="$($DOCKER ps -aq --filter label=crdb-chaos.node=1)"
  if [ -n "$ids" ]; then $DOCKER rm -f $ids >/dev/null; fi
  $DC down -v --remove-orphans
  echo "Stopped."
}

cmd_status() { curl -fsS "$PANEL/api/state" | python3 -c "$STATUS_PY"; }

cmd_prefetch() {
  $DOCKER pull "docker.io/cockroachdb/cockroach:${CRDB_VERSION}"
  $DOCKER pull docker.io/library/haproxy:lts
  $DC build
}

cmd_prove() {
  command -v node >/dev/null 2>&1 || { echo "The proof run needs Node.js 22+ (for the browser checks)." >&2; exit 1; }
  if [ ! -x .venv/bin/pytest ]; then
    python3 -m venv .venv
    .venv/bin/pip install -q -r controlplane/requirements-dev.txt
  fi
  .venv/bin/python tools/prove.py "$@"
}

cmd_test() {
  if [ ! -x .venv/bin/pytest ]; then
    python3 -m venv .venv
    .venv/bin/pip install -q -r controlplane/requirements-dev.txt
  fi
  .venv/bin/pytest "$@"
}

case "${1:-}" in
  up) shift; cmd_up "$@";;
  down) shift; cmd_down "$@";;
  reset) shift; cmd_down; cmd_up "$@";;
  status) cmd_status;;
  prefetch) cmd_prefetch;;
  test) shift; cmd_test "$@";;
  prove) shift; cmd_prove "$@";;
  wait) shift; wait_ready "${1:?wait needs a preset name}" "${2:-$DEPLOYMENT_MODEL}";;
  *) usage;;
esac
