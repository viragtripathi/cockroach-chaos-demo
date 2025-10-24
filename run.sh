
#!/usr/bin/env bash
set -euo pipefail

DC="docker compose"
if ! command -v docker >/dev/null 2>&1; then
  if command -v podman >/dev/null 2>&1; then
    DC="podman compose"
  else
    echo "Neither docker nor podman found. Please install one."
    exit 1
  fi
fi

echo "Bringing up CockroachDB + HAProxy + Toxiproxy + Backend..."
$DC -f docker-compose.yml up -d --build

echo "Waiting for backend to be available at http://localhost:8088 ..."
for i in {1..60}; do
  if curl -fsS http://localhost:8088/ >/dev/null; then
    echo "Backend is up!"
    break
  fi
  sleep 1
done

echo "Open the Chaos Panel: http://localhost:8088"
echo "Point your Banko AI app at: cockroachdb://root@localhost:26257/defaultdb?sslmode=disable"
