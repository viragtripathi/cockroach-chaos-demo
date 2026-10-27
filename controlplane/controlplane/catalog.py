"""Known cloud regions: city label, map coordinates and real zone names."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class CatalogRegion:
    provider: str
    name: str
    label: str
    location: tuple[float, float]
    zones: tuple[str, ...]


class Catalog:
    def __init__(self, regions: dict[tuple[str, str], CatalogRegion]):
        self._regions = regions

    def lookup(self, provider: str, name: str) -> CatalogRegion | None:
        return self._regions.get((provider, name))

    def providers(self) -> list[str]:
        return sorted({provider for provider, _ in self._regions})

    def regions_for(self, provider: str) -> list[CatalogRegion]:
        return sorted((r for (p, _), r in self._regions.items() if p == provider), key=lambda r: r.name)


def parse_catalog(data: dict) -> Catalog:
    regions: dict[tuple[str, str], CatalogRegion] = {}
    for provider, entries in (data.get("providers") or {}).items():
        for name, entry in (entries or {}).items():
            lat, lon = entry["location"]
            regions[(provider, name)] = CatalogRegion(
                provider=provider,
                name=name,
                label=str(entry["label"]),
                location=(float(lat), float(lon)),
                zones=tuple(str(z) for z in entry["zones"]),
            )
    return Catalog(regions)


def load_catalog(path: Path) -> Catalog:
    with open(path, encoding="utf-8") as f:
        return parse_catalog(yaml.safe_load(f) or {})
