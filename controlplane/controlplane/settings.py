"""Runtime configuration for the control plane, read once from the environment."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class Settings:
    crdb_version: str
    preset: str
    subnet: str
    network: str
    max_nodes: int
    presets_dir: Path
    catalog_path: Path
    data_dir: Path
    haproxy_cfg_dir: Path
    haproxy_container: str
    web_dir: Path
    time_until_store_dead: str
    crdb_license: str | None
    crdb_org: str | None
    control_token: str | None = None
    trigger_token: str | None = None
    helper_image: str = "crdb-chaos-control-plane:local"
    panel_port: int = 8088
    tls_dir: Path = Path("/app/tls")
    model: str = "multi-region"  # deployment model for new clusters: multi-region | single-region

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> "Settings":
        def get(name: str, default: str) -> str:
            return env.get(name, "").strip() or default

        return cls(
            crdb_version=get("CRDB_VERSION", "v26.3.2"),
            preset=get("PRESET", "aws"),
            subnet=get("CHAOS_SUBNET", "10.77.0.0/16"),
            network=get("CHAOS_NETWORK", "crdb-chaos"),
            max_nodes=int(get("MAX_NODES", "15")),
            presets_dir=Path(get("PRESETS_DIR", "/app/presets")),
            catalog_path=Path(get("CATALOG_PATH", "/app/catalog/regions.yaml")),
            data_dir=Path(get("DATA_DIR", "/data")),
            haproxy_cfg_dir=Path(get("HAPROXY_CFG_DIR", "/haproxy")),
            haproxy_container=get("HAPROXY_CONTAINER", "haproxy"),
            web_dir=Path(get("WEB_DIR", "/app/web")),
            time_until_store_dead=get("TIME_UNTIL_STORE_DEAD", "1m15s"),
            crdb_license=env.get("CRDB_LICENSE", "").strip() or None,
            crdb_org=env.get("CRDB_ORG", "").strip() or None,
            control_token=env.get("CONTROL_TOKEN", "").strip() or None,
            trigger_token=env.get("TRIGGER_TOKEN", "").strip() or None,
            helper_image=get("HELPER_IMAGE", "crdb-chaos-control-plane:local"),
            panel_port=int(get("PANEL_PORT", "8088")),
            tls_dir=Path(get("TLS_DIR", "/app/tls")),
            model=get("DEPLOYMENT_MODEL", "multi-region").lower(),
        )
