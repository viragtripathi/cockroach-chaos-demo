"""Where a Payments worker lives and where it connects.

On a multi-region cluster every account and every worker belongs to a region. A worker connects to a node in its
own region, so each statement of a transfer is local and only the commit crosses a region. When its region has no
usable node it connects to the nearest region that has one, and tries home again on every reconnect.
"""
from __future__ import annotations

import math
import random
from typing import Sequence


def distance_km(a: Sequence[float], b: Sequence[float]) -> float:
    """Great-circle distance between two (lat, lon) points."""
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(h))


def home_of(index: int, regions: Sequence[str]) -> str:
    """The region worker `index` serves: workers go round robin over the regions."""
    return regions[index % len(regions)]


def workers_by_region(workers: int, regions: Sequence[str]) -> dict[str, int]:
    counts = {r: 0 for r in regions}
    for i in range(workers):
        counts[home_of(i, regions)] += 1
    return counts


def region_order(home: str, regions: Sequence[dict]) -> list[str]:
    """Home first, then the other regions nearest first. Regions without coordinates come after those with them,
    the primary region first among them, then in the order given."""
    by_name = {r["name"]: r for r in regions}
    origin = (by_name.get(home) or {}).get("location")
    others = [r for r in regions if r["name"] != home]

    def key(item):
        index, r = item
        loc = r.get("location")
        if origin and loc:
            return (0, distance_km(origin, loc), index)
        return (1, 0 if r.get("primary") else 1, index)

    return [home] + [r["name"] for _, r in sorted(enumerate(others), key=key)]


def usable(n: dict) -> bool:
    """A node a worker can connect to: live, running, on the network and not cut off from the other regions."""
    return (n.get("liveness") == "live" and n.get("runtime") == "running"
            and not n.get("partitioned") and not n.get("isolated"))


def gateway_candidates(home: str, snapshot: dict, rng: random.Random) -> list[tuple[str, str]]:
    """(ip, region) of the nodes a worker homed in `home` should try, in order: its own region's usable nodes in
    random order, then the nearest other region's, and so on. Empty when nothing usable is known."""
    regions = (snapshot or {}).get("regions") or []
    nodes = [n for n in (snapshot or {}).get("nodes") or [] if usable(n)]
    out: list[tuple[str, str]] = []
    for region in region_order(home, regions):
        mine = [n for n in nodes if n.get("region") == region]
        rng.shuffle(mine)
        out += [(n["ip"], region) for n in mine]
    return out
