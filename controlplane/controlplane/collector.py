"""Background thread that rebuilds the snapshot every interval."""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
from typing import Callable

from .docker_ops import DockerError
from .snapshot import assemble_snapshot

log = logging.getLogger(__name__)

DECOMMISSIONED = 2  # liveness membership: 0 active, 1 decommissioning, 2 decommissioned


def _fetch_json(host: str, path: str) -> dict:
    with urllib.request.urlopen(f"http://{host}:8080{path}", timeout=2) as resp:
        return json.load(resp)


def parse_status(nodes: dict, liveness: dict, now: float) -> tuple[list[dict], dict]:
    """Node rows and range health from the HTTP status API. Telemetry over HTTP never shows up as SQL traffic.
    Live means the node's liveness record hasn't expired (what crdb_internal.gossip_nodes.is_live reports); the
    endpoint's 'statuses' also folds in the answering node's own connections, which flap after a restart."""
    records = {int(r["nodeId"]): r for r in liveness.get("livenesses") or []}
    rows, unavailable, underreplicated = [], 0, 0
    for n in nodes.get("nodes") or []:
        node_id = int(n["desc"]["nodeId"])
        record = records.get(node_id, {})
        if str(record.get("membership", 0)) in (str(DECOMMISSIONED), "DECOMMISSIONED"):
            continue  # gone for good; its name may belong to a new node now
        metrics = [s.get("metrics") or {} for s in n.get("storeStatuses") or []]
        expires = int((record.get("expiration") or {}).get("wallTime") or 0) / 1e9
        live = expires > now
        rows.append({"node_id": node_id, "address": n["desc"]["address"]["addressField"], "is_live": live,
                     "ranges": int(sum(m.get("replicas", 0) for m in metrics)),
                     "leases": int(sum(m.get("replicas.leaseholders", 0) for m in metrics))})
        if live:  # a dead node's last report is stale
            unavailable += sum(m.get("ranges.unavailable", 0) for m in metrics)
            underreplicated += sum(m.get("ranges.underreplicated", 0) for m in metrics)
    return rows, {"unavailable": int(unavailable), "underreplicated": int(underreplicated)}


class Collector:
    def __init__(self, docker, provisioner, operations, *, status_host: str, fetch_json: Callable = _fetch_json,
                 interval: float = 1.5, clock: Callable[[], float] = time.time, chaos=None, network: str | None = None,
                 extras: Callable[[], dict] | None = None):
        self._docker = docker
        self._provisioner = provisioner
        self._ops = operations
        self._status_host = status_host
        self._fetch = fetch_json
        self._interval = interval
        self._clock = clock
        self._chaos = chaos
        self._network = network
        self._extras = extras
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._latest = assemble_snapshot(None, [], None, None, None, clock())

    def latest(self) -> dict:
        with self._lock:
            return self._latest

    def refresh(self) -> dict:
        topo = self._provisioner.topology
        try:
            containers = self._docker.list_nodes()
        except DockerError:
            containers = []
        sql_nodes, ranges = (None, None)
        if topo is not None:
            hosts = [self._status_host] + [n.ip for n in topo.nodes]
            sql_nodes, ranges = self._query(hosts)
        flags = self._chaos.flags() if self._chaos else None
        latency = self._chaos.latency_setting() if (self._chaos and topo) else None
        snap = assemble_snapshot(topo, containers, sql_nodes, ranges, self._ops.snapshot(), self._clock(),
                                 network=self._network, flags=flags, latency=latency)
        if self._extras:
            snap.update(self._extras())
        with self._lock:
            self._latest = snap
        return snap

    def _query(self, hosts):
        for host in hosts:
            try:
                return parse_status(self._fetch(host, "/_status/nodes"), self._fetch(host, "/_admin/v1/liveness"), self._clock())
            except Exception as e:  # node down or partitioned: try the next one
                log.debug("status query via %s failed: %s", host, e)
        return None, None

    def start(self) -> None:
        def loop():
            while not self._stop.is_set():
                try:
                    self.refresh()
                except Exception:
                    log.exception("snapshot refresh failed")
                self._stop.wait(self._interval)
        threading.Thread(target=loop, daemon=True, name="collector").start()

    def stop(self) -> None:
        self._stop.set()
