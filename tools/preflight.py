#!/usr/bin/env python3
"""Host-side checks before ./run.sh starts the stack. Standard library only."""
from __future__ import annotations

import argparse
import ipaddress
import json
import re
import socket
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

PROJECT = "crdb-chaos"
LEGACY_NAMES = {"crdb-e1a", "crdb-e1b", "crdb-w2a", "crdb-w2b", "crdb-c1", "crdb-init", "crdb-bootstrap",
                "toxiproxy-east", "toxiproxy-west", "toxiproxy-central", "chaos-backend"}
NODE_MIB, BASE_MIB = 1100, 700  # measured: a node with --max-go-memory=1GiB settles around 1 GiB RSS


@dataclass
class Check:
    name: str
    ok: bool
    message: str
    level: str = "error"  # error | warn


def docker_cli(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=30)


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("0.0.0.0", port))
        except OSError:
            return True
    return False


def _subnets(net: dict) -> list[str]:
    """Docker: IPAM.Config[].Subnet. Podman: subnets[].subnet."""
    docker_style = [c.get("Subnet") for c in (net.get("IPAM") or {}).get("Config") or []]
    podman_style = [s.get("subnet") for s in net.get("subnets") or []]
    return [cidr for cidr in docker_style + podman_style if cidr]


def _free_slash16(taken) -> str:
    """First 10.x.0.0/16 (x from 77) or 172.x.0.0/16 that overlaps none of the existing networks."""
    candidates = [f"10.{x}.0.0/16" for x in range(77, 100)] + [f"172.{x}.0.0/16" for x in range(29, 32)]
    for cidr in candidates:
        net = ipaddress.ip_network(cidr)
        if not any(net.overlaps(t) for t in taken):
            return cidr
    return "10.99.0.0/16"


def _mem_total(raw: str) -> int:
    """Docker: MemTotal. Podman: host.memTotal. 0 when unknown."""
    try:
        data = json.loads(raw or "{}")
        return int(data.get("MemTotal") or (data.get("host") or {}).get("memTotal") or 0)
    except (ValueError, TypeError, AttributeError):
        return 0


def preset_nodes(path: Path, model: str = "multi-region") -> int:
    text = "\n".join(line.split("#", 1)[0] for line in path.read_text(encoding="utf-8").splitlines())
    counts = [int(n) for n in re.findall(r"\bnodes:\s*(\d+)", text)]
    if model == "single-region":
        return max(3, counts[0] if counts else 0)  # the primary region, at least one node per zone
    return sum(counts)


def run_checks(*, nodes: int, subnet: str, panel_port: int,
               docker: Callable[[list[str]], subprocess.CompletedProcess] = docker_cli,
               in_use: Callable[[int], bool] = port_in_use) -> list[Check]:
    # The host `docker` may be Docker's CLI or Podman's; both print JSON with --format json.
    info = docker(["info", "--format", "json"])
    if info.returncode != 0:
        return [Check("runtime", False, "Docker/Podman is not reachable. Start Docker Desktop or run `podman machine start`.")]
    checks = [Check("runtime", True, "runtime reachable")]

    listing = docker(["ps", "-a", "--format", '{{.Names}}\t{{.Label "com.docker.compose.project"}}'])
    rows = [(line.split("\t") + [""])[:2] for line in listing.stdout.splitlines() if line.strip()]
    ours = any(project == PROJECT for _, project in rows)
    legacy = sorted(name for name, project in rows if name in LEGACY_NAMES or (name == "haproxy" and project != PROJECT))
    if legacy:
        checks.append(Check("legacy", False, f"The pre-redesign demo is still running ({', '.join(legacy)}). "
                                             "Stop it with: ./run.sh down --legacy"))
    else:
        checks.append(Check("legacy", True, "no pre-redesign demo running"))

    if not ours and not legacy:
        busy = [p for p in (26257, 8080, 8404, panel_port) if in_use(p)]
        checks.append(Check("ports", not busy,
                            f"ports in use by another program: {', '.join(map(str, busy))}" if busy
                            else f"ports 26257, 8080, 8404 and {panel_port} are free"))

    try:
        wanted = ipaddress.ip_network(subnet)
    except ValueError:
        checks.append(Check("subnet", False, f"CHAOS_SUBNET {subnet!r} is not a valid network"))
    else:
        clashes, taken, unreadable = [], [], False
        ids = docker(["network", "ls", "-q"]).stdout.split()
        try:
            networks = json.loads(docker(["network", "inspect", *ids]).stdout or "[]") if ids else []
        except ValueError:
            networks, unreadable = [], True
        for net in networks:
            name = net.get("Name") or net.get("name")
            if name == PROJECT:
                continue
            for cidr in _subnets(net):
                taken.append(ipaddress.ip_network(cidr, strict=False))
                if ipaddress.ip_network(cidr, strict=False).overlaps(wanted):
                    clashes.append(f"{name} ({cidr})")
        if unreadable:
            checks.append(Check("subnet", True, f"could not read the existing networks; assuming {subnet} is free",
                                level="warn"))
        else:
            checks.append(Check("subnet", not clashes,
                                f"CHAOS_SUBNET {subnet} overlaps {', '.join(clashes)}; set another /16 in .env, "
                                f"e.g. CHAOS_SUBNET={_free_slash16(taken + [wanted])}" if clashes else f"subnet {subnet} is free"))

    total_mib = _mem_total(info.stdout) // (1024 * 1024)
    need = nodes * NODE_MIB + BASE_MIB
    enough = not total_mib or total_mib >= need
    checks.append(Check("memory", enough,
                        f"memory looks sufficient for {nodes} nodes" if enough
                        else f"~{need} MiB recommended for {nodes} nodes but the runtime has {total_mib} MiB; "
                             "consider --preset small", level="warn"))
    return checks


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preset-file", type=Path, required=True)
    parser.add_argument("--subnet", required=True)
    parser.add_argument("--panel-port", type=int, required=True)
    parser.add_argument("--model", default="multi-region")
    args = parser.parse_args(argv)
    failed = False
    for c in run_checks(nodes=preset_nodes(args.preset_file, args.model), subnet=args.subnet, panel_port=args.panel_port):
        print(f"  {'✓' if c.ok else ('!' if c.level == 'warn' else '✗')} {c.message}")
        failed |= not c.ok and c.level == "error"
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
