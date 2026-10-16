"""The single state document every viewer receives: built by the collector, served by the API."""
from __future__ import annotations

from typing import Sequence

from .docker_ops import ContainerInfo
from .topology import Topology


PROVISIONING = ("provision", "rebuild", "adopt")


def _liveness(row, runtime: str, telemetry_ok: bool) -> str:
    if not telemetry_ok:
        return "unknown"
    if row is not None:
        return "live" if row["is_live"] else "dead"
    return "joining" if runtime == "running" else "dead"


def assemble_snapshot(topo: Topology | None, containers: Sequence[ContainerInfo], sql_nodes: Sequence[dict] | None,
                      ranges: dict | None, operation: dict | None, now: float, *, network: str | None = None,
                      flags: dict | None = None, latency: str | None = None) -> dict:
    telemetry_ok = sql_nodes is not None
    by_name = {c.name: c for c in containers}
    # a decommissioned node's name can come back on a new node: the highest node id is the current one
    by_host = {str(r["address"]).rsplit(":", 1)[0]: r for r in sorted(sql_nodes or [], key=lambda r: r["node_id"])}
    nodes, regions = [], []
    if topo:
        for n in topo.nodes:
            c = by_name.get(n.name)
            runtime = "missing" if c is None else ("running" if c.running else "stopped")
            row = by_host.get(n.name)
            nodes.append({
                "name": n.name, "region": n.region, "zone": n.zone, "provider": n.provider, "ip": n.ip,
                "runtime": runtime, "liveness": _liveness(row, runtime, telemetry_ok),
                "node_id": row["node_id"] if row else None,
                "replicas": row["ranges"] if row else None,
                "leases": row["leases"] if row else None,
                "partitioned": bool(network) and runtime == "running" and network not in c.ips,
                "brownout": (flags or {}).get(n.name, {}).get("brownout"),
                "isolated": bool((flags or {}).get(n.name, {}).get("isolated")),
            })
        for r in topo.regions:
            members = [x for x in nodes if x["region"] == r.name]
            live = sum(x["liveness"] == "live" for x in members)
            state = ("unknown" if not telemetry_ok else
                     "healthy" if live == len(members) else "down" if live == 0 else "degraded")
            regions.append({
                "name": r.name, "provider": r.provider, "label": r.label,
                "location": list(r.location) if r.location else None,
                "primary": r.primary, "secondary": r.secondary, "zones": list(r.zones),
                "state": state, "nodes": [x["name"] for x in members],
            })

    unavailable = (ranges or {}).get("unavailable")
    underreplicated = (ranges or {}).get("underreplicated")
    live_nodes = sum(x["liveness"] == "live" for x in nodes)
    if operation and operation.get("status") == "running" and operation.get("kind") in PROVISIONING:
        status = "provisioning"
    elif topo is None:
        status = "empty"
    elif not telemetry_ok:
        status = "unreachable"
    elif unavailable:
        status = "critical"
    elif live_nodes == len(nodes):
        status = "healthy"
    else:
        status = "degraded" if live_nodes else "critical"

    return {
        "ts": now,
        "cluster": None if topo is None else {
            "preset": topo.preset, "display_name": topo.display_name, "cluster_id": topo.cluster_id,
            "crdb_version": topo.crdb_version, "survival_goal": topo.survival_goal,
            "providers": sorted({r.provider for r in topo.regions}), "status": status,
            "live_nodes": live_nodes, "total_nodes": len(nodes), "warnings": list(topo.warnings),
            "latency": latency or topo.latency, "model": topo.model,
        },
        "regions": regions,
        "nodes": nodes,
        "ranges": {"unavailable": unavailable, "underreplicated": underreplicated},
        "operation": operation,
        "telemetry": {"sql": "ok" if telemetry_ok else "delayed"},
    }
