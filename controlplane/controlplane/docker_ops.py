"""Thin wrapper around the docker CLI. Works against Docker and Podman's Docker-compatible API."""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass, field
from typing import Callable, Sequence

from .topology import NodeSpec, Topology

NODE_LABEL = "crdb-chaos.node"

Runner = Callable[[list[str], float], subprocess.CompletedProcess]


class DockerError(RuntimeError):
    """A docker command failed; the message carries the command and its stderr."""


def cli_runner(args: list[str], timeout: float) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)


@dataclass(frozen=True)
class ContainerInfo:
    name: str
    id: str
    running: bool
    status: str
    labels: dict = field(default_factory=dict)
    ips: dict = field(default_factory=dict)  # network name -> IP
    exit_code: int | None = None  # set once the container has exited


def node_run_args(spec: NodeSpec, topo: Topology, join: Sequence[str], network: str) -> list[str]:
    return [
        "run", "-d", "--name", spec.name, "--hostname", spec.name,
        "--network", network, "--ip", spec.ip,
        "--label", f"{NODE_LABEL}=1",
        "--label", f"crdb-chaos.cluster={topo.cluster_id}",
        "--label", f"crdb-chaos.region={spec.region}",
        "--label", f"crdb-chaos.zone={spec.zone}",
        "--label", f"crdb-chaos.provider={spec.provider}",
        "--label", f"crdb-chaos.ordinal={spec.ordinal}",
        "--label", f"crdb-chaos.ip={spec.ip}",
        topo.image, "start", "--insecure",
        f"--advertise-addr={spec.name}:26257",
        "--listen-addr=0.0.0.0:26257",
        "--http-addr=0.0.0.0:8080",
        f"--locality=cloud={spec.provider},region={spec.region},zone={spec.zone}",
        f"--cluster-name={topo.cluster_id}",
        f"--join={','.join(join)}",
        f"--cache={topo.cache}",
        f"--max-sql-memory={topo.sql_memory}",
        f"--max-go-memory={topo.go_memory}",
        # Keep log files small: a laptop demo doesn't need the defaults (100 MiB files, 1 GiB groups per channel).
        "--log=file-defaults: {dir: /cockroach/cockroach-data/logs, max-file-size: 10MiB, max-group-size: 50MiB}",
    ]


def parse_inspect(raw: str) -> list[ContainerInfo]:
    out = []
    for c in json.loads(raw or "[]"):
        state = c.get("State") or {}
        networks = (c.get("NetworkSettings") or {}).get("Networks") or {}
        out.append(ContainerInfo(
            name=str(c.get("Name", "")).lstrip("/"),
            id=str(c.get("Id", "")),
            running=bool(state.get("Running")),
            status=str(state.get("Status", "")),
            labels=dict((c.get("Config") or {}).get("Labels") or {}),
            ips={name: v.get("IPAddress", "") for name, v in networks.items()},
            exit_code=None if state.get("Running") or state.get("ExitCode") is None else int(state["ExitCode"]),
        ))
    return out


class Docker:
    def __init__(self, run: Runner = cli_runner):
        self._run = run

    def _call(self, args: list[str], timeout: float = 60.0, check: bool = True) -> subprocess.CompletedProcess:
        try:
            proc = self._run(list(args), timeout)
        except subprocess.TimeoutExpired as e:
            raise DockerError(f"docker {' '.join(args)} timed out after {timeout:g}s") from e
        if check and proc.returncode != 0:
            raise DockerError(f"docker {' '.join(args)} failed: {(proc.stderr or proc.stdout or '').strip()}")
        return proc

    def list_nodes(self) -> list[ContainerInfo]:
        names = self._call(["ps", "-a", "--filter", f"label={NODE_LABEL}=1", "--format", "{{.Names}}"], timeout=15).stdout.split()
        if not names:
            return []
        return parse_inspect(self._call(["inspect", *names], timeout=15).stdout)

    def image_present(self, image: str) -> bool:
        return self._call(["image", "inspect", image], timeout=15, check=False).returncode == 0

    def pull(self, image: str) -> None:
        self._call(["pull", image], timeout=900)

    def run_node(self, spec: NodeSpec, topo: Topology, join: Sequence[str], network: str) -> None:
        self._call(node_run_args(spec, topo, join, network), timeout=120)

    def start(self, name: str) -> None:
        self._call(["start", name], timeout=60)

    def stop(self, name: str, grace: int = 20) -> None:
        self._call(["stop", "-t", str(grace), name], timeout=grace + 30)

    def network_disconnect(self, network: str, name: str) -> None:
        self._call(["network", "disconnect", network, name], timeout=30)

    def network_connect(self, network: str, name: str, ip: str) -> None:
        self._call(["network", "connect", "--ip", ip, network, name], timeout=30)

    def run_helper(self, name: str, script: str, *, image: str) -> subprocess.CompletedProcess:
        """Run a throwaway container inside a node's network namespace (used for tc/netem)."""
        return self._call(["run", "--rm", "--network", f"container:{name}", "--cap-add", "NET_ADMIN",
                           "--entrypoint", "sh", image, "-c", script], timeout=60)

    def remove(self, name: str) -> None:
        self._call(["rm", "-f", name], timeout=60, check=False)

    def exec(self, name: str, cmd: Sequence[str], timeout: float = 60.0, check: bool = True) -> subprocess.CompletedProcess:
        return self._call(["exec", name, *cmd], timeout=timeout, check=check)

    def kill(self, name: str, signal: str) -> None:
        self._call(["kill", "-s", signal, name], timeout=30)
