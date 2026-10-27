import subprocess
from pathlib import Path

import pytest

from controlplane.docker_ops import DockerError
from controlplane.lb import LoadBalancer, render_haproxy_config
from controlplane.topology import NodeSpec

NODES = [NodeSpec("us-east-1a-n1", "us-east-1", "us-east-1a", "aws", 1, "10.77.1.11"),
         NodeSpec("us-west-2a-n1", "us-west-2", "us-west-2a", "aws", 1, "10.77.2.11")]


class FakeDocker:
    def __init__(self, container_up=True, pid1="haproxy", valid=True):
        self.container_up, self.pid1, self.valid = container_up, pid1, valid
        self.execs, self.kills = [], []

    def exec(self, name, cmd, timeout=60.0, check=True):
        self.execs.append(cmd)
        if not self.container_up:
            raise DockerError("container not running")
        if cmd[0] == "cat":
            return subprocess.CompletedProcess(cmd, 0, self.pid1 + "\n", "")
        if cmd[0] == "haproxy" and not self.valid:
            raise DockerError("config invalid")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    def kill(self, name, signal):
        self.kills.append((name, signal))


def test_config_routes_sql_and_console_to_every_node():
    cfg = render_haproxy_config(NODES)
    assert "server us-east-1a-n1 10.77.1.11:26257" in cfg
    assert "server us-west-2a-n1 10.77.2.11:8080" in cfg
    assert "option pgsql-check user root" in cfg
    assert "on-marked-down shutdown-sessions" in cfg
    assert "balance first" in cfg and "option httpchk GET /health" in cfg
    assert "hard-stop-after 10m" in cfg
    for port in ("*:26257", "*:8080", "*:8404"):
        assert f"bind {port}" in cfg


def test_empty_topology_still_renders_all_frontends_without_servers():
    cfg = render_haproxy_config([])
    assert "backend crdb_sql_nodes" in cfg and "backend crdb_console_nodes" in cfg
    assert "  server " not in cfg


def test_apply_validates_swaps_and_reloads(tmp_path: Path):
    d = FakeDocker()
    LoadBalancer(d, tmp_path).apply(NODES)
    assert "10.77.1.11:26257" in (tmp_path / "haproxy.cfg").read_text()
    assert not (tmp_path / "haproxy.cfg.next").exists()
    assert ["haproxy", "-c", "-q", "-f", "/cfg/haproxy.cfg.next"] in d.execs
    assert d.kills == [("haproxy", "USR2")]


def test_apply_does_not_signal_while_haproxy_is_still_waiting_for_its_first_config(tmp_path: Path):
    d = FakeDocker(pid1="sh")
    LoadBalancer(d, tmp_path).apply(NODES)
    assert (tmp_path / "haproxy.cfg").exists()
    assert d.kills == []


def test_apply_writes_without_validating_when_haproxy_is_down(tmp_path: Path):
    d = FakeDocker(container_up=False)
    LoadBalancer(d, tmp_path).apply(NODES)
    assert (tmp_path / "haproxy.cfg").exists() and d.kills == []


def test_invalid_config_is_rejected_and_the_live_one_is_kept(tmp_path: Path):
    (tmp_path / "haproxy.cfg").write_text("live", encoding="utf-8")
    with pytest.raises(DockerError):
        LoadBalancer(FakeDocker(valid=False), tmp_path).apply(NODES)
    assert (tmp_path / "haproxy.cfg").read_text() == "live"


def test_ensure_config_only_writes_when_missing(tmp_path: Path):
    lb = LoadBalancer(FakeDocker(container_up=False), tmp_path)
    lb.ensure_config()
    first = (tmp_path / "haproxy.cfg").read_text()
    assert "backend crdb_sql_nodes" in first
    (tmp_path / "haproxy.cfg").write_text("keep", encoding="utf-8")
    lb.ensure_config()
    assert (tmp_path / "haproxy.cfg").read_text() == "keep"
