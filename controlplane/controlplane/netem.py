"""Per-node network shaping with tc/netem: realistic inter-region latency, brownouts and region isolation.

Each node gets a prio qdisc: band 1 carries everything else (clients, infrastructure, its own region),
and one band per remote region (matched by that region's /24) carries half the region-pair round trip.
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from .topology import NodeSpec, RegionSpec, Topology

FIBER_KM_PER_MS = 200.0
ROUTE_FACTOR = 1.5
BASE_RTT_MS = 5.0
UNKNOWN_RTT_MS = 60.0


@dataclass(frozen=True)
class Brownout:
    delay_ms: int = 700
    jitter_ms: int = 200
    loss_pct: float = 2.0


def _distance_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    lat1, lon1, lat2, lon2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371 * math.asin(math.sqrt(h))


def rtt_ms(a: RegionSpec, b: RegionSpec, scale: float) -> float:
    if not a.location or not b.location:
        return UNKNOWN_RTT_MS * scale
    return (2 * _distance_km(a.location, b.location) / FIBER_KM_PER_MS * ROUTE_FACTOR + BASE_RTT_MS) * scale


def latency_scale(setting: str) -> float | None:
    """'off' -> no shaping; 'realistic' -> 1.0; a number -> that multiplier."""
    if setting == "off":
        return None
    try:
        return float(setting)
    except (TypeError, ValueError):
        return 1.0


def _netem(delay_ms: float, brownout: Brownout | None, loss_pct: float) -> str:
    delay = round(delay_ms + (brownout.delay_ms if brownout else 0))
    parts = ["limit 10000", f"delay {delay}ms"]
    if brownout and brownout.jitter_ms:
        parts.append(f"{brownout.jitter_ms}ms")
    if loss_pct:
        parts.append(f"loss {loss_pct:g}%" if loss_pct == 100 else f"loss {loss_pct:.1f}%")
    return " ".join(parts)


def tc_script(node: NodeSpec, topo: Topology, *, prefix: str, scale: float | None, brownout: Brownout | None,
              isolated: bool, dev: str = "eth0") -> str:
    lines = [f"tc qdisc del dev {dev} root 2>/dev/null; true"]
    if scale is None and brownout is None and not isolated:
        return "\n".join(lines)
    own = next(r for r in topo.regions if r.name == node.region)
    remotes = [r for r in topo.regions if r.name != node.region]
    if not remotes and brownout is None:
        return "\n".join(lines)  # single region: nothing to delay, nothing to isolate from
    # prio needs at least 2 bands; with no remote regions band 2 simply stays unused.
    lines.append(f"tc qdisc add dev {dev} root handle 1: prio bands {max(2, len(remotes) + 1)} priomap {' '.join(['0'] * 16)}")
    lines.append(f"tc qdisc add dev {dev} parent 1:1 handle 10: netem {_netem(0, brownout, brownout.loss_pct if brownout else 0)}")
    for band, region in enumerate(remotes, start=2):
        one_way = rtt_ms(own, region, scale) / 2 if scale else 0
        loss = 100 if isolated else (brownout.loss_pct if brownout else 0)
        lines.append(f"tc qdisc add dev {dev} parent 1:{band} handle {band * 10}: netem {_netem(one_way, brownout, loss)}")
        lines.append(f"tc filter add dev {dev} parent 1: protocol ip prio 1 u32 match ip dst {prefix}.{region.index}.0/24 flowid 1:{band}")
    return "\n".join(lines)


class Netem:
    """Applies a node's shaping by running tc in a helper container that shares the node's network."""

    def __init__(self, docker, *, prefix: str, image: str):
        self._docker = docker
        self._prefix = prefix
        self._image = image

    def apply(self, node: NodeSpec, topo: Topology, *, scale: float | None, brownout: Brownout | None, isolated: bool) -> None:
        script = tc_script(node, topo, prefix=self._prefix, scale=scale, brownout=brownout, isolated=isolated)
        self._docker.run_helper(node.name, script, image=self._image)
