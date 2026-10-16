"""Survival budget: would taking these nodes down make some data unavailable under the cluster's survival goal?"""
from __future__ import annotations

from .topology import Topology


def predict_unavailable(topo: Topology, down: set[str]) -> str | None:
    """Return a warning when `down` exceeds what the survival goal tolerates, else None."""
    by_region: dict[str, list[str]] = {}
    for n in topo.nodes:
        by_region.setdefault(n.region, []).append(n.name)
    regions_down = [r for r, names in by_region.items() if names and all(n in down for n in names)]
    if topo.survival_goal == "region":
        if len(regions_down) > 1:
            return (f"{len(regions_down)} regions would be down ({', '.join(regions_down)}). REGION survival tolerates one; "
                    "some ranges lose quorum and become unavailable.")
        # REGION survival keeps 5 replicas, at most 2 per region: 3 lost replicas anywhere can break quorum.
        lost = {r: sum(n in down for n in names) for r, names in by_region.items()}
        if sum(min(v, 2) for v in lost.values()) >= 3:
            hit = ", ".join(f"{v} in {r}" for r, v in lost.items() if v)
            return (f"Nodes down: {hit}. Some ranges keep 2 replicas in each of those regions, so 3 of their 5 "
                    "replicas would be gone and they become unavailable.")
        return None
    if topo.survival_goal == "zone":
        primary = by_region[topo.primary.name]
        lost = sum(n in down for n in primary)
        if lost >= 2:
            return (f"{lost} of {len(primary)} nodes in the primary region would be down. ZONE survival tolerates one; "
                    "ranges homed there lose quorum.")
        return None
    lost = sum(n.name in down for n in topo.nodes)
    if lost >= 2:
        return f"{lost} nodes would be down. With plain 3× replication, ranges with two replicas on them lose quorum."
    return None
