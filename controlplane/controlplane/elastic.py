"""Live topology changes: add or decommission nodes, add or remove regions, survival goal and primary region."""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import replace
from typing import Callable

from .bootstrap import DATABASES, _ident, _literal
from .operations import Operation
from .retry import retry_with_backoff
from .topology import (PresetError, Topology, with_added_node, with_added_region, without_nodes, without_region)

log = logging.getLogger(__name__)

JOIN_ATTEMPTS = 120
DRAIN_ATTEMPTS = 900  # replicas move at the cluster's pace; 15 minutes is generous for a demo-sized dataset
SQL_ATTEMPTS = 30
SETTLE_ATTEMPTS = 36  # x 5 s: CockroachDB marks a restarted store "suspect" for a while (30 s by default)
TRANSIENT_REFUSALS = ("suspect", "throttled")  # decommission pre-check refusals that clear up on their own


class ElasticError(ValueError):
    """The change can't start; the message is shown to the presenter."""


def _background(fn: Callable[[], None]) -> None:
    import threading
    threading.Thread(target=fn, daemon=True).start()


class Elastic:
    def __init__(self, docker, provisioner, lb, operations, chaos, settings, *, connect: Callable, catalog,
                 sleep: Callable[[float], None] = time.sleep,
                 run_in_background: Callable[[Callable[[], None]], None] | None = None):
        # Called inside remove_region's operation: before the DROP REGION statements (the Payments workload moves
        # its rows out, since CockroachDB refuses to drop a region that still homes rows) and after the change.
        self.before_region_removal: list = []
        self.after_region_removal: list = []
        self._docker = docker
        self._provisioner = provisioner
        self._lb = lb
        self._ops = operations
        self._chaos = chaos
        self._settings = settings
        self._connect = connect
        self._catalog = catalog
        self._sleep = sleep
        self._run_bg = run_in_background or _background

    # -- entry points: validate now (ElasticError), then run as an operation in the background --

    def add_node(self, region: str) -> None:
        topo = self._topology()
        try:
            new_topo, node = with_added_node(topo, region, subnet=self._settings.subnet, max_nodes=self._settings.max_nodes)
        except PresetError as e:
            raise ElasticError(str(e)) from e

        def work(op):
            self._ops.step(op, f"Allocating capacity in {region}")
            with self._undo_on_failure([node.name]):
                self._docker.run_node(node, new_topo, self._seeds(topo), self._settings.network)
                self._ops.step(op, f"Waiting for {node.name} to join")
                self._wait_live(node.name)
            self._finish_change(op, new_topo)
            self._ops.step(op, f"{node.name} is serving; replicas and leases rebalance onto it")
        self._submit("add-node", work, topo)

    def decommission(self, name: str) -> None:
        topo = self._topology()
        node = next((n for n in topo.nodes if n.name == name), None)
        if node is None:
            raise ElasticError(f"no node named {name!r}")
        if sum(n.region == node.region for n in topo.nodes) == 1:
            raise ElasticError(f"{name} is the last node in {node.region}; remove the region instead")

        def work(op):
            self._drain(op, topo, [name])
            self._finish_change(op, without_nodes(topo, {name}))
        self._submit("decommission", work, topo)

    def add_region(self, name: str, provider: str, nodes: int = 3, label: str | None = None, location=None) -> None:
        topo = self._topology()
        try:
            new_topo = with_added_region(topo, self._catalog, name=name, provider=provider, nodes=nodes, label=label,
                                         location=location, subnet=self._settings.subnet, max_nodes=self._settings.max_nodes)
        except PresetError as e:
            raise ElasticError(str(e)) from e
        region = new_topo.regions[-1]
        added = [n for n in new_topo.nodes if n.region == region.name]

        def work(op):
            self._ops.step(op, f"Allocating capacity in {region.name}")
            seeds = self._seeds(topo)
            with self._undo_on_failure([n.name for n in added]):
                for n in added:
                    self._docker.run_node(n, new_topo, seeds, self._settings.network)
                self._ops.step(op, f"Waiting for {region.name} to join")
                self._wait_region(region.name)
            stmts = []
            if region.location:
                stmts.append('UPSERT INTO system.locations ("localityKey", "localityValue", latitude, longitude) '
                             f"VALUES ('region', {_literal(region.name)}, {region.location[0]}, {region.location[1]})")
            if topo.survival_goal != "none":
                stmts += [f"ALTER DATABASE {db} ADD REGION IF NOT EXISTS {_ident(region.name)}" for db in DATABASES]
            self._ops.step(op, f"Adding {region.name} to the multi-region databases")
            self._run_sql(stmts)
            self._finish_change(op, new_topo)
            if topo.survival_goal == "zone" and len(new_topo.regions) >= 3:
                self._ops.step(op, "The cluster now qualifies for REGION survival")
        self._submit("add-region", work, topo)

    def remove_region(self, name: str) -> None:
        topo = self._topology()
        try:
            new_topo = without_region(topo, name)
        except PresetError as e:
            raise ElasticError(str(e)) from e
        region = next(r for r in topo.regions if r.name == name)
        goal = topo.survival_goal
        if goal == "region" and len(new_topo.regions) < 3:
            goal = "zone"
        new_topo = replace(new_topo, survival_goal=goal)

        def work(op):
            try:
                # First, before anything changes: a refused evacuation leaves the cluster as it was.
                if self.before_region_removal:
                    self._ops.step(op, f"Checking Payments accounts in {name}")
                    for hook in self.before_region_removal:
                        hook(name)
                if topo.survival_goal != "none" and (region.secondary or goal != topo.survival_goal):
                    stmts = []
                    if region.secondary:
                        stmts += [f"ALTER DATABASE {db} DROP SECONDARY REGION IF EXISTS" for db in DATABASES]
                    if goal != topo.survival_goal:
                        self._ops.step(op, "Two regions remain: switching to ZONE survival")
                        stmts += [f"ALTER DATABASE {db} SURVIVE ZONE FAILURE" for db in DATABASES]
                    self._run_sql(stmts)
                    # Record what already happened so a retry after a later failure starts from the truth.
                    self._provisioner.commit(replace(topo, survival_goal=goal, regions=tuple(
                        replace(r, secondary=r.secondary and r.name != name) for r in topo.regions)))
                if topo.survival_goal != "none":
                    self._ops.step(op, f"Removing {name} from the multi-region databases")
                    self._run_sql([f"ALTER DATABASE {db} DROP REGION IF EXISTS {_ident(name)}" for db in DATABASES])
                self._drain(op, topo, [n.name for n in topo.nodes if n.region == name])
                self._finish_change(op, new_topo)
            finally:
                # Whatever happened, Payments may run again; its failure to restart never changes the outcome.
                for hook in self.after_region_removal:
                    try:
                        hook(name)
                    except Exception as e:
                        log.warning("after-removal hook failed: %s", e)
                        self._ops.step(op, f"Note: Payments did not restart on its own: {e}")
        self._submit("remove-region", work, topo)

    def set_survival(self, goal: str) -> None:
        topo = self._topology()
        goal = goal.strip().lower()
        if goal not in ("zone", "region"):
            raise ElasticError("survival goal must be zone or region")
        if topo.survival_goal == "none":
            raise ElasticError("this cluster has a single region; add regions first")
        if goal == "region" and len(topo.regions) < 3:
            raise ElasticError("REGION survival needs at least 3 regions")
        primary_nodes = sum(n.region == topo.primary.name for n in topo.nodes)
        if goal == "zone" and primary_nodes < 3:
            raise ElasticError(f"ZONE survival keeps 3 replicas in the primary region: it needs 3 nodes in "
                               f"{topo.primary.name} (has {primary_nodes}); add a node there first")

        def work(op):
            self._ops.step(op, f"Switching to {goal.upper()} survival")
            self._run_sql([f"ALTER DATABASE {db} SURVIVE {goal.upper()} FAILURE" for db in DATABASES])
            self._provisioner.commit(replace(topo, survival_goal=goal))
            self._ops.step(op, "Replicas are moving to meet the new goal")
        self._submit("survival", work, topo)

    def set_primary(self, name: str) -> None:
        topo = self._topology()
        if not any(r.name == name for r in topo.regions):
            raise ElasticError(f"no region named {name!r}")
        if topo.primary.name == name:
            raise ElasticError(f"{name} is already the primary region")
        if topo.survival_goal == "none":
            raise ElasticError("this cluster has a single region")
        regions = tuple(replace(r, primary=r.name == name, secondary=r.secondary and r.name != name) for r in topo.regions)

        def work(op):
            self._ops.step(op, f"Making {name} the primary region")
            stmts = []
            if topo.secondary and topo.secondary.name == name:
                stmts += [f"ALTER DATABASE {db} DROP SECONDARY REGION IF EXISTS" for db in DATABASES]
            stmts += [f"ALTER DATABASE {db} SET PRIMARY REGION {_ident(name)}" for db in DATABASES]
            self._run_sql(stmts)
            self._provisioner.commit(replace(topo, regions=regions))
            self._ops.step(op, "Leaseholders are moving to the new primary")
        self._submit("primary", work, topo)

    # -- internals --

    def _topology(self) -> Topology:
        topo = self._provisioner.topology
        if topo is None:
            raise ElasticError("no cluster yet")
        return topo

    def _submit(self, kind: str, work: Callable[[Operation], None], planned_from: Topology) -> None:
        op = self._ops.start(kind)  # OperationBusy propagates to the caller

        def run():
            try:
                if self._provisioner.topology is not planned_from:
                    raise RuntimeError("the cluster changed while this was starting; try again")
                work(op)
            except Exception as e:  # surfaced to the presenter
                log.exception("%s failed", kind)
                self._ops.finish(op, error=str(e) or type(e).__name__)
            else:
                self._ops.finish(op)
        self._run_bg(run)

    def _seeds(self, topo: Topology) -> list[str]:
        """Join through nodes that are up right now; the first nodes may be in a region that is down."""
        live = self._live_hosts()
        return [f"{name}:26257" for name in (live or [n.name for n in topo.nodes])[:3]]

    @contextmanager
    def _undo_on_failure(self, names: list[str]):
        """Remove nodes started by a change that failed, so a retry can reuse their names and addresses."""
        known = {n.name for n in self._topology().nodes}
        for c in self._docker.list_nodes():
            if c.name in names and c.name not in known:  # left over from an interrupted earlier attempt
                self._docker.remove(c.name)
        try:
            yield
        except BaseException:
            for name in names:
                try:
                    self._docker.remove(name)
                except Exception as e:
                    log.warning("could not remove %s after a failed change: %s", name, e)
            raise

    def _finish_change(self, op: Operation, topo: Topology) -> None:
        self._ops.step(op, "Updating the global load balancer")
        self._lb.apply(topo.nodes)
        self._provisioner.commit(topo)
        for warning in self._chaos.apply_all():
            self._ops.step(op, f"Note: {warning}")

    def _live_hosts(self, exclude=()) -> list[str]:
        topo = self._topology()
        running = {c.name for c in self._docker.list_nodes() if c.running and self._settings.network in c.ips}
        return [n.name for n in topo.nodes if n.name in running and n.name not in exclude]

    def _conn(self, exclude=()):
        last: Exception | None = None
        for _ in range(SQL_ATTEMPTS):
            for host in self._live_hosts(exclude):
                try:
                    return self._connect(host)
                except Exception as e:
                    last = e
            self._sleep(1)
        raise RuntimeError(f"no node accepted a SQL connection: {last}")

    def _query(self, sql: str, *, one=False, exclude=()):
        def once():
            conn = self._conn(exclude)
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    return cur.fetchone() if one else cur.fetchall()
            finally:
                conn.close()
        return retry_with_backoff(once, max_retries=6, initial_backoff=0.2, sleep=self._sleep)

    def _run_sql(self, statements) -> None:
        """Every statement here is idempotent (IF EXISTS, SET, UPSERT), so transient errors retry them one by one."""
        for sql in statements:
            def once(sql=sql):
                conn = self._conn()
                try:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                finally:
                    conn.close()
            retry_with_backoff(once, max_retries=6, initial_backoff=0.2, sleep=self._sleep)

    def _wait_live(self, name: str) -> None:
        sql = f"SELECT count(*) FROM crdb_internal.gossip_nodes WHERE address = {_literal(name + ':26257')} AND is_live"
        for _ in range(JOIN_ATTEMPTS):
            try:
                if self._query(sql, one=True, exclude=(name,))[0]:
                    return
            except Exception as e:
                log.debug("waiting for %s: %s", name, e)
            self._sleep(1)
        raise RuntimeError(f"{name} did not join the cluster")

    def _wait_region(self, region: str) -> None:
        for _ in range(JOIN_ATTEMPTS):
            try:
                if region in {r[0] for r in self._query("SELECT region FROM [SHOW REGIONS FROM CLUSTER]")}:
                    return
            except Exception as e:
                log.debug("waiting for %s: %s", region, e)
            self._sleep(1)
        raise RuntimeError(f"{region} did not join the cluster")

    def _drain(self, op: Operation, topo: Topology, names: list[str]) -> None:
        rows = self._query("SELECT n.node_id, n.address FROM crdb_internal.gossip_nodes n "
                           "JOIN crdb_internal.gossip_liveness l USING (node_id) WHERE l.membership = 'active'",
                           exclude=names)
        by_host = {str(addr).rsplit(":", 1)[0]: int(node_id) for node_id, addr in rows}
        ids = [str(by_host[n]) for n in names if n in by_host]
        survivors = self._live_hosts(exclude=names)
        if not survivors:
            raise RuntimeError("no live node left to run the decommission from")
        host = survivors[0]
        if ids:
            self._ops.step(op, f"Decommissioning {', '.join(names)}")
            self._decommission(host, topo, ids, "none", op=op)
            label = names[0] if len(names) == 1 else f"{len(names)} nodes"
            for i in range(DRAIN_ATTEMPTS):
                left = int(self._query("SELECT COALESCE(sum(ranges), 0) FROM crdb_internal.gossip_nodes "
                                       f"WHERE node_id IN ({', '.join(ids)})", one=True, exclude=names)[0])
                self._ops.progress(op, f"Moving replicas off {label}: {left} left")
                if left == 0:
                    break
                self._sleep(1)
            else:
                raise RuntimeError(f"replicas did not drain from {label}; check the DB Console")
            self._decommission(host, topo, ids, "all")
        self._ops.step(op, "Releasing capacity")
        if self._chaos is not None:
            self._chaos.forget(names)
        for name in names:
            self._docker.stop(name)
            self._docker.remove(name)

    def _decommission(self, host: str, topo: Topology, ids: list[str], wait: str, op: Operation | None = None) -> None:
        cmd = ["/cockroach/cockroach", "node", "decommission", *ids, f"--wait={wait}", "--insecure",
               f"--host={host}:26257", f"--cluster-name={topo.cluster_id}"]
        for attempt in range(SETTLE_ATTEMPTS):
            proc = self._docker.exec(host, cmd, timeout=120, check=False)
            if proc.returncode == 0:
                return
            output = (proc.stderr or proc.stdout).strip()
            # Right after chaos, a restarted node's store is "suspect" and the pre-check refuses until it settles.
            if not any(marker in output for marker in TRANSIENT_REFUSALS) or attempt == SETTLE_ATTEMPTS - 1:
                raise RuntimeError(f"decommission refused: {output}")
            if op is not None and attempt == 0:
                self._ops.step(op, "Waiting for recently restarted nodes to settle before moving replicas")
            self._sleep(5)
