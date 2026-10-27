"""Presets -> validated topology: regions, zones, node names and static IPs."""
from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, replace
from pathlib import Path

import yaml

from .catalog import Catalog

NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")
SURVIVAL_GOALS = ("auto", "zone", "region", "none")
MODELS = ("multi-region", "single-region")  # deployment models; multi-region is the default
MAX_NODES_PER_REGION = 12
# go: the Go heap limit. CockroachDB's default (2.25 x sql) is ~576 MiB here, below what an idle node with a few
# hundred replicas needs, which makes the Go GC run nonstop and starves the node of CPU.
DEFAULT_MEMORY = {"cache": "256MiB", "sql": "256MiB", "go": "1GiB"}


class PresetError(ValueError):
    """A preset that cannot become a cluster. The message is shown to the presenter as-is."""


@dataclass(frozen=True)
class RegionSpec:
    name: str
    provider: str
    nodes: int
    label: str
    location: tuple[float, float] | None
    zones: tuple[str, ...]
    primary: bool
    secondary: bool
    index: int  # 1-based; selects the region's /24


@dataclass(frozen=True)
class NodeSpec:
    name: str
    region: str
    zone: str
    provider: str
    ordinal: int  # 1-based within its region
    ip: str


@dataclass(frozen=True)
class Topology:
    preset: str
    display_name: str
    cluster_id: str
    crdb_version: str
    survival_goal: str  # resolved: "region" | "zone" | "none"
    latency: str
    cache: str
    sql_memory: str
    regions: tuple[RegionSpec, ...]
    nodes: tuple[NodeSpec, ...]
    warnings: tuple[str, ...] = ()
    go_memory: str = "1GiB"
    model: str = "multi-region"

    @property
    def primary(self) -> RegionSpec:
        return next(r for r in self.regions if r.primary)

    @property
    def secondary(self) -> RegionSpec | None:
        return next((r for r in self.regions if r.secondary), None)

    @property
    def image(self) -> str:
        return f"docker.io/cockroachdb/cockroach:{self.crdb_version}"


def default_zones(provider: str, region: str, count: int = 3) -> tuple[str, ...]:
    letters = "abcdefgh"
    if provider == "aws":
        return tuple(f"{region}{letters[i]}" for i in range(count))
    if provider in ("azure", "ibm"):
        return tuple(f"{region}-{i + 1}" for i in range(count))
    if provider == "oci":
        return tuple(f"{region}-ad{i + 1}" for i in range(count))
    return tuple(f"{region}-{letters[i]}" for i in range(count))


def resolve_survival_goal(requested: str, regions) -> str:
    if requested != "auto":
        return requested
    total = sum(r.nodes for r in regions)
    primary = next(r for r in regions if r.primary)
    if len(regions) >= 3 and total >= 5 and primary.nodes >= 2:
        return "region"
    if primary.nodes >= 3:
        return "zone"
    return "none"


def survival_warnings(goal: str, regions) -> list[str]:
    total = sum(r.nodes for r in regions)
    primary = next(r for r in regions if r.primary)
    warnings = []
    if goal == "region" and (len(regions) < 3 or total < 5 or primary.nodes < 2):
        warnings.append("REGION survival needs 3+ regions, 5+ nodes and 2+ nodes in the primary region; "
                        "ranges will stay under-replicated")
    if goal == "zone" and primary.nodes < 3:
        warnings.append("ZONE survival needs 3+ nodes in the primary region; ranges will stay under-replicated")
    return warnings


def subnet_prefix(subnet: str) -> str:
    try:
        net = ipaddress.ip_network(subnet)
    except ValueError as e:
        raise PresetError(f"CHAOS_SUBNET {subnet!r} is not a valid network") from e
    if net.version != 4 or net.prefixlen != 16:
        raise PresetError(f"CHAOS_SUBNET must be an IPv4 /16, got {subnet}")
    a, b, _, _ = str(net.network_address).split(".")
    return f"{a}.{b}"


def node_specs(regions, subnet: str) -> tuple[NodeSpec, ...]:
    prefix = subnet_prefix(subnet)
    plan = [(r, r.zones[(k - 1) % len(r.zones)], k) for r in regions for k in range(1, r.nodes + 1)]
    names = [f"{zone}-n{k}" for _, zone, k in plan]
    duplicates = {n for n in names if names.count(n) > 1}
    specs = []
    for (region, zone, k), name in zip(plan, names):
        if name in duplicates:
            name = f"{region.provider}-{name}"
        if len(name) > 63:
            raise PresetError(f"node name {name!r} is longer than 63 characters")
        specs.append(NodeSpec(name=name, region=region.name, zone=zone, provider=region.provider,
                              ordinal=k, ip=f"{prefix}.{region.index}.{10 + k}"))
    return tuple(specs)


def _location(raw, preset: str, region: str) -> tuple[float, float] | None:
    if raw is None:
        return None
    try:
        lat, lon = (float(v) for v in raw)
    except (TypeError, ValueError):
        raise PresetError(f"preset {preset}: region {region} location must be [lat, lon]") from None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise PresetError(f"preset {preset}: region {region} location {raw} is out of range")
    return (lat, lon)


def parse_preset(name: str, data, catalog: Catalog, *, default_version: str, subnet: str, max_nodes: int,
                 model: str = "multi-region") -> Topology:
    if not isinstance(data, dict):
        raise PresetError(f"preset {name}: expected a mapping at the top level")
    raw_regions = data.get("regions")
    if not isinstance(raw_regions, list) or not raw_regions:
        raise PresetError(f"preset {name}: 'regions' must be a non-empty list")

    seen: set[str] = set()
    regions: list[RegionSpec] = []
    for i, raw in enumerate(raw_regions, start=1):
        if not isinstance(raw, dict):
            raise PresetError(f"preset {name}: region #{i} must be a mapping")
        rname = str(raw.get("name", "")).strip()
        if not NAME_RE.match(rname):
            raise PresetError(f"preset {name}: region name {rname!r} must be lowercase letters, digits and hyphens (max 40)")
        if rname in seen:
            raise PresetError(f"preset {name}: region {rname} is listed twice")
        seen.add(rname)
        provider = str(raw.get("provider", "custom")).strip().lower()
        nodes = raw.get("nodes", 3)
        if not isinstance(nodes, int) or isinstance(nodes, bool) or not 1 <= nodes <= MAX_NODES_PER_REGION:
            raise PresetError(f"preset {name}: region {rname} needs 'nodes' between 1 and {MAX_NODES_PER_REGION}")
        known = catalog.lookup(provider, rname)
        if raw.get("zones") is not None and not (isinstance(raw["zones"], list) and all(isinstance(z, str) for z in raw["zones"])):
            raise PresetError(f"preset {name}: region {rname} 'zones' must be a list of zone names")
        zones = tuple(raw["zones"]) if raw.get("zones") else (known.zones if known else default_zones(provider, rname))
        for zone in zones:
            if not NAME_RE.match(zone):
                raise PresetError(f"preset {name}: zone name {zone!r} in {rname} must be lowercase letters, digits and hyphens")
        location = _location(raw.get("location"), name, rname) or (known.location if known else None)
        regions.append(RegionSpec(
            name=rname, provider=provider, nodes=nodes,
            label=str(raw.get("label") or (known.label if known else rname)),
            location=location, zones=zones,
            primary=bool(raw.get("primary")), secondary=bool(raw.get("secondary")), index=i,
        ))

    if sum(r.primary for r in regions) > 1:
        raise PresetError(f"preset {name}: only one region can be primary")
    if not any(r.primary for r in regions):
        regions[0] = replace(regions[0], primary=True)
    if sum(r.secondary for r in regions) > 1:
        raise PresetError(f"preset {name}: only one region can be secondary")
    if model not in MODELS:
        raise PresetError(f"unknown deployment model {model!r}: use {' or '.join(MODELS)}")
    if model == "single-region":
        # The preset's primary region on its own, with at least one node per availability zone so it survives
        # a zone outage. Same names and clouds as the multi-region version of the preset.
        primary = next(r for r in regions if r.primary)
        regions = [replace(primary, nodes=max(primary.nodes, min(3, len(primary.zones))), secondary=False, index=1)]
    if any(r.primary and r.secondary for r in regions):
        raise PresetError(f"preset {name}: the primary region cannot also be secondary")
    total = sum(r.nodes for r in regions)
    if total > max_nodes:
        raise PresetError(f"preset {name}: {total} nodes exceeds MAX_NODES={max_nodes}")

    requested = str(data.get("survival_goal", "auto")).strip().lower()
    if requested not in SURVIVAL_GOALS:
        raise PresetError(f"preset {name}: survival_goal must be one of {', '.join(SURVIVAL_GOALS)}")
    if model == "single-region" and requested == "region":
        requested = "auto"  # one region can't survive losing it: fall back to the best goal it can have
    if requested == "region" and len(regions) < 3:
        raise PresetError(f"preset {name}: REGION survival needs at least 3 regions (has {len(regions)})")
    cluster_id = str(data.get("cluster_id") or name)
    if not NAME_RE.match(cluster_id):
        raise PresetError(f"preset {name}: cluster_id {cluster_id!r} must be lowercase letters, digits and hyphens")
    if data.get("node_memory") is not None and not isinstance(data["node_memory"], dict):
        raise PresetError(f"preset {name}: 'node_memory' must be a mapping like {{cache: 256MiB, sql: 256MiB, go: 1GiB}}")
    memory = {**DEFAULT_MEMORY, **(data.get("node_memory") or {})}
    frozen = tuple(regions)
    return Topology(
        preset=name,
        display_name=str(data.get("display_name") or name),
        cluster_id=cluster_id,
        crdb_version=str(data.get("crdb_version") or default_version),
        survival_goal=resolve_survival_goal(requested, frozen),
        latency=str(data.get("latency", "realistic")),
        cache=str(memory["cache"]),
        sql_memory=str(memory["sql"]),
        go_memory=str(memory["go"]),
        regions=frozen,
        nodes=node_specs(frozen, subnet),
        warnings=tuple(survival_warnings(requested, frozen)) if requested != "auto" else (),
        model="single-region" if len(frozen) == 1 else "multi-region",
    )


def load_preset(presets_dir: Path, name: str, catalog: Catalog, *, default_version: str, subnet: str, max_nodes: int,
                model: str = "multi-region") -> Topology:
    name = name.strip().lower()
    if not NAME_RE.match(name):
        raise PresetError(f"unknown preset {name!r}")
    for candidate in (presets_dir / f"{name}.yaml", presets_dir / "custom" / f"{name}.yaml"):
        if candidate.is_file():
            try:
                data = yaml.safe_load(candidate.read_text(encoding="utf-8"))
            except yaml.YAMLError as e:
                raise PresetError(f"preset {name}: invalid YAML ({e})") from e
            return parse_preset(name, data, catalog, default_version=default_version, subnet=subnet, max_nodes=max_nodes,
                                model=model)
    raise PresetError(f"unknown preset {name!r}")


def list_presets(presets_dir: Path) -> list[str]:
    names = {p.stem for p in presets_dir.glob("*.yaml")}
    names |= {p.stem for p in (presets_dir / "custom").glob("*.yaml")}
    return sorted(names)


# -- elasticity: derive a new topology from the running one --

def _with_regions(topo: Topology, regions, nodes, max_nodes: int) -> Topology:
    if len(nodes) > max_nodes:
        raise PresetError(f"{len(nodes)} nodes exceeds MAX_NODES={max_nodes}")
    counts = {r.name: sum(n.region == r.name for n in nodes) for r in regions}
    return replace(topo, regions=tuple(replace(r, nodes=counts[r.name]) for r in regions), nodes=tuple(nodes),
                   model="single-region" if len(regions) == 1 else "multi-region")


def _unique_name(name: str, provider: str, taken: set[str]) -> str:
    return name if name not in taken else f"{provider}-{name}"


def with_added_node(topo: Topology, region_name: str, *, subnet: str, max_nodes: int) -> tuple[Topology, NodeSpec]:
    region = next((r for r in topo.regions if r.name == region_name), None)
    if region is None:
        raise PresetError(f"no region named {region_name!r}")
    members = [n for n in topo.nodes if n.region == region_name]
    if len(members) >= MAX_NODES_PER_REGION:
        raise PresetError(f"region {region_name} already has the maximum of {MAX_NODES_PER_REGION} nodes")
    used = {n.ordinal for n in members}
    ordinal = next(k for k in range(1, MAX_NODES_PER_REGION + 2) if k not in used)
    zone = min(region.zones, key=lambda z: (sum(n.zone == z for n in members), region.zones.index(z)))
    name = _unique_name(f"{zone}-n{ordinal}", region.provider, {n.name for n in topo.nodes})
    node = NodeSpec(name=name, region=region_name, zone=zone, provider=region.provider, ordinal=ordinal,
                    ip=f"{subnet_prefix(subnet)}.{region.index}.{10 + ordinal}")
    return _with_regions(topo, topo.regions, [*topo.nodes, node], max_nodes), node


def without_nodes(topo: Topology, names: set[str]) -> Topology:
    return _with_regions(topo, topo.regions, [n for n in topo.nodes if n.name not in names], len(topo.nodes))


def with_added_region(topo: Topology, catalog: Catalog, *, name: str, provider: str, nodes: int, subnet: str,
                      max_nodes: int, label: str | None = None, location=None) -> Topology:
    name, provider = name.strip(), (provider or "custom").strip().lower()
    if not NAME_RE.match(name):
        raise PresetError(f"region name {name!r} must be lowercase letters, digits and hyphens (max 40)")
    if any(r.name == name for r in topo.regions):
        raise PresetError(f"region {name} is already part of the cluster")
    if not isinstance(nodes, int) or not 1 <= nodes <= MAX_NODES_PER_REGION:
        raise PresetError(f"a region needs between 1 and {MAX_NODES_PER_REGION} nodes")
    known = catalog.lookup(provider, name)
    index = max(r.index for r in topo.regions) + 1
    if index > 250:
        raise PresetError("no address space left for another region")
    region = RegionSpec(name=name, provider=provider, nodes=nodes, label=label or (known.label if known else name),
                        location=_location(location, "custom", name) or (known.location if known else None),
                        zones=known.zones if known else default_zones(provider, name),
                        primary=False, secondary=False, index=index)
    taken = {n.name for n in topo.nodes}
    added = [replace(n, name=_unique_name(n.name, provider, taken)) for n in node_specs([region], subnet)]
    return _with_regions(topo, [*topo.regions, region], [*topo.nodes, *added], max_nodes)


def without_region(topo: Topology, name: str) -> Topology:
    region = next((r for r in topo.regions if r.name == name), None)
    if region is None:
        raise PresetError(f"no region named {name!r}")
    if region.primary:
        raise PresetError(f"{name} is the primary region; make another region primary first")
    regions = [r for r in topo.regions if r.name != name]
    return _with_regions(topo, regions, [n for n in topo.nodes if n.region != name], len(topo.nodes))
