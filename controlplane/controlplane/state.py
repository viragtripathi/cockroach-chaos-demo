"""Persisted control-plane state: the active topology and whether provisioning finished."""
from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict
from pathlib import Path

from .topology import NodeSpec, RegionSpec, Topology

STATE_FILE = "state.json"


def topology_to_dict(t: Topology) -> dict:
    return asdict(t)


def topology_from_dict(d: dict) -> Topology:
    regions = tuple(
        RegionSpec(**{**r, "location": tuple(r["location"]) if r.get("location") else None, "zones": tuple(r["zones"])})
        for r in d["regions"]
    )
    nodes = tuple(NodeSpec(**n) for n in d["nodes"])
    return Topology(**{**d, "regions": regions, "nodes": nodes, "warnings": tuple(d.get("warnings", ()))})


def load_state(data_dir: Path) -> dict:
    path = data_dir / STATE_FILE
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_state(data_dir: Path, state: dict) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=data_dir, prefix=".state-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, data_dir / STATE_FILE)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
