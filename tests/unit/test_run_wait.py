"""run.sh's wait loop against a fake panel: a failed attempt is retried once, not reported forever."""
import json
import os
import shutil
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.skipif(not (shutil.which("docker") or shutil.which("podman")), reason="run.sh needs a container CLI")

FAILED = {"operation": {"id": "a", "status": "failed", "error": "Could not pull image x"}, "cluster": {"preset": "aws", "status": "unreachable"}}
HEALTHY = {"operation": {"id": "b", "status": "done", "error": None}, "cluster": {"preset": "aws", "status": "healthy"}}
FAILED_AGAIN = {"operation": {"id": "b", "status": "failed", "error": "Could not pull image x"}, "cluster": {"preset": "aws", "status": "unreachable"}}


def fake_panel(after_rebuild):
    state = {"current": FAILED, "posts": []}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            body = json.dumps(state["current"]).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            state["posts"].append(self.rfile.read(int(self.headers["Content-Length"])).decode())
            state["current"] = after_rebuild
            self.send_response(202)
            self.end_headers()
            self.wfile.write(b'{"accepted": true}')

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, state


def run_wait(port):
    env = {**os.environ, "PANEL_PORT": str(port)}
    return subprocess.run([str(ROOT / "run.sh"), "wait", "aws"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=60)


def test_a_previous_failure_is_retried_once_and_then_succeeds():
    server, state = fake_panel(HEALTHY)
    try:
        proc = run_wait(server.server_port)
    finally:
        server.shutdown()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "Ready: preset 'aws'" in proc.stdout
    assert state["posts"] == ['{"preset":"aws","model":"multi-region"}']


def test_a_retry_that_fails_again_exits_with_the_new_error():
    server, state = fake_panel(FAILED_AGAIN)
    try:
        proc = run_wait(server.server_port)
    finally:
        server.shutdown()
    assert proc.returncode == 1
    assert "Provisioning failed: Could not pull image x" in proc.stderr
    assert len(state["posts"]) == 1


def test_a_running_cluster_with_another_model_is_rebuilt():
    single = {"operation": {"id": "c", "status": "done", "error": None},
              "cluster": {"preset": "aws", "model": "single-region", "status": "healthy"}}
    server, state = fake_panel(HEALTHY)
    state["current"] = single
    try:
        proc = run_wait(server.server_port)  # asks for aws, multi-region (the default)
    finally:
        server.shutdown()
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert state["posts"] == ['{"preset":"aws","model":"multi-region"}']
