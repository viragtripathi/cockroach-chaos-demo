"""Cluster bootstrap: settings, map locations and multi-region databases, generated from the topology."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Iterable

from .retry import classify, retry_with_backoff
from .topology import NAME_RE, Topology

DATABASES = ("defaultdb", "chaos")


@dataclass(frozen=True)
class Statement:
    sql: str
    optional: bool = False  # failures become warnings instead of stopping bootstrap
    secret: str | None = None  # a value in sql (a license key) that must never reach an error, a log or the stage

    def shown(self, text: str | None = None) -> str:
        """The statement, or any message about it, with the secret blanked out."""
        text = self.sql if text is None else text
        return text.replace(self.secret, "<redacted>") if self.secret else text


class BootstrapError(RuntimeError):
    def __init__(self, statement: Statement, cause: Exception):
        super().__init__(f"bootstrap failed on: {statement.shown()} ({statement.shown(str(cause))})")
        self.statement = statement
        self.cause = cause


def _ident(name: str) -> str:
    if not NAME_RE.match(name):
        raise ValueError(f"unsafe identifier {name!r}")
    return f'"{name}"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def license_statements(license: str | None, org: str | None = None, *, optional: bool = False) -> list[Statement]:
    """The license key, preceded by the organization when one is given (current keys carry it; older ones must match
    it). Without a key, CockroachDB throttles a multi-node cluster to 5 open transactions 7 days after init."""
    if not license:
        return []
    out = [Statement(f"SET CLUSTER SETTING cluster.organization = {_literal(org)}", optional=optional)] if org else []
    out.append(Statement(f"SET CLUSTER SETTING enterprise.license = {_literal(license)}", optional=optional, secret=license))
    return out


def bootstrap_statements(topo: Topology, *, time_until_store_dead: str,
                         license: str | None = None, org: str | None = None) -> list[Statement]:
    out: list[Statement] = license_statements(license, org)
    out.append(Statement(f"SET CLUSTER SETTING server.time_until_store_dead = {_literal(time_until_store_dead)}", optional=True))
    out.append(Statement("SET CLUSTER SETTING kv.rangefeed.enabled = true"))
    # On a laptop, the host can stall the virtual disk for seconds (swap, sleep, other VMs). Log the stall instead of
    # killing the node: a demo node dying for that reason looks like a database failure that never happened.
    out.append(Statement("SET CLUSTER SETTING storage.max_sync_duration.fatal.enabled = false", optional=True))
    located = [r for r in topo.regions if r.location]
    if located:
        values = ", ".join(f"('region', {_literal(r.name)}, {r.location[0]}, {r.location[1]})" for r in located)
        out.append(Statement(f'UPSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) VALUES {values}'))
    out.append(Statement("CREATE DATABASE IF NOT EXISTS chaos"))
    if topo.survival_goal != "none":
        for db in DATABASES:
            out.append(Statement(f"ALTER DATABASE {db} SET PRIMARY REGION {_ident(topo.primary.name)}"))
            for region in topo.regions:
                if not region.primary:
                    out.append(Statement(f"ALTER DATABASE {db} ADD REGION IF NOT EXISTS {_ident(region.name)}"))
            if topo.survival_goal == "region" and topo.secondary:
                out.append(Statement(f"ALTER DATABASE {db} SET SECONDARY REGION {_ident(topo.secondary.name)}"))
            out.append(Statement(f"ALTER DATABASE {db} SURVIVE {topo.survival_goal.upper()} FAILURE"))
    return out


def run_bootstrap(conn, statements: Iterable[Statement], log: Callable[[str], None] = print,
                  sleep: Callable[[float], None] = time.sleep) -> list[str]:
    """Execute in order. Required failures raise BootstrapError; optional ones come back as warnings."""
    warnings: list[str] = []
    with conn.cursor() as cur:
        for st in statements:
            try:
                # Same connection, so only 40001 (never committed, connection still usable) is retried here.
                retry_with_backoff(lambda: cur.execute(st.sql), max_retries=5, initial_backoff=0.2, sleep=sleep,
                                   retry_if=lambda e: classify(e) == "serialization")
            except Exception as e:  # driver errors; kept generic so this module stays testable without a server
                if not st.optional:
                    raise BootstrapError(st, e) from e
                warnings.append(f"{st.shown()}: {st.shown(str(e))}")
                log(f"bootstrap warning: {st.shown()}: {st.shown(str(e))}")
    return warnings
