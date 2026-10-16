"""Creates, adopts and rebuilds the CockroachDB nodes described by a topology."""
from __future__ import annotations

import logging
import time
from typing import Callable

from .bootstrap import bootstrap_statements, run_bootstrap
from .docker_ops import DockerError
from .operations import Operation, Operations
from .state import load_state, save_state, topology_from_dict, topology_to_dict
from .topology import NodeSpec, Topology

log = logging.getLogger(__name__)

INIT_ATTEMPTS = 90
SQL_ATTEMPTS = 60
REGION_ATTEMPTS = 90


class ProvisionError(RuntimeError):
    """Provisioning cannot continue; the message is shown to the presenter."""


class Provisioner:
    def __init__(self, docker, lb, operations: Operations, settings, *, connect: Callable,
                 sleep: Callable[[float], None] = time.sleep):
        self._docker = docker
        self._lb = lb
        self._ops = operations
        self._settings = settings
        self._connect = connect
        self._sleep = sleep
        self.topology: Topology | None = None
        self.on_ready: list[Callable[[], None]] = []  # e.g. re-apply network shaping
        self.on_teardown: list[Callable[[], None]] = []  # e.g. forget chaos flags of nodes about to go

    # -- entry points: synchronous, never raise, outcome recorded on the operation --

    def ensure(self, preset_topology: Topology) -> None:
        """Startup: adopt a finished cluster if one exists, otherwise build a clean one."""
        state = load_state(self._settings.data_dir)
        containers = self._docker.list_nodes()
        if containers and state.get("provisioned") and state.get("topology"):
            self._adopt(topology_from_dict(state["topology"]))
        elif containers:
            self.rebuild(preset_topology, kind="provision")  # leftovers from an interrupted run
        else:
            self.provision(preset_topology)

    def adopt_saved(self) -> bool:
        """Reconnect to the cluster recorded in state, if one finished provisioning and its nodes exist."""
        state = load_state(self._settings.data_dir)
        if state.get("provisioned") and state.get("topology") and self._docker.list_nodes():
            self._adopt(topology_from_dict(state["topology"]))
            return True
        return False

    def provision(self, topo: Topology, kind: str = "provision") -> None:
        op = self._ops.start(kind)
        self._run(op, lambda: self._provision(op, topo))

    def rebuild(self, topo: Topology, kind: str = "rebuild") -> None:
        op = self._ops.start(kind)

        def work():
            # Mark unfinished first: a restart mid-teardown must rebuild, never adopt a half-deleted cluster.
            save_state(self._settings.data_dir, {"topology": topology_to_dict(topo), "provisioned": False})
            for hook in self.on_teardown:
                hook()
            self._ops.step(op, "Draining the global load balancer")
            self._lb.apply([])
            self._ops.step(op, "Releasing existing capacity")
            for c in self._docker.list_nodes():
                self._docker.remove(c.name)
            self._provision(op, topo)

        self._run(op, work)

    def commit(self, topo: Topology) -> None:
        """Persist a topology changed in place (elasticity, survival goal)."""
        self.topology = topo
        save_state(self._settings.data_dir, {"topology": topology_to_dict(topo), "provisioned": True})

    # -- internals --

    def _ready(self, op: Operation) -> None:
        for hook in self.on_ready:
            try:
                hook()
            except Exception as e:  # shaping is best effort; the cluster itself is fine
                log.warning("ready hook failed: %s", e)
                self._ops.step(op, f"Note: {e}")

    def _run(self, op: Operation, work: Callable[[], None]) -> None:
        try:
            work()
        except Exception as e:  # surfaced to the presenter, never crashes the control plane
            log.exception("%s failed", op.kind)
            self._ops.finish(op, error=str(e))
        else:
            self._ops.finish(op)

    def _adopt(self, topo: Topology) -> None:
        op = self._ops.start("adopt")

        def work():
            self._ops.step(op, "Reconnecting to the running cluster")
            self.topology = topo
            names = {n.name for n in topo.nodes}
            for c in self._docker.list_nodes():
                if c.name in names and not c.running:  # stopped with the host or VM
                    self._docker.start(c.name)
            self._lb.apply(topo.nodes)
            self._ready(op)

        self._run(op, work)

    def _provision(self, op: Operation, topo: Topology) -> None:
        self.topology = topo
        save_state(self._settings.data_dir, {"topology": topology_to_dict(topo), "provisioned": False})
        if not self._docker.image_present(topo.image):
            self._ops.step(op, f"Downloading CockroachDB {topo.crdb_version}")
            try:
                self._docker.pull(topo.image)
            except DockerError as e:
                raise ProvisionError(f"Could not pull image {topo.image}: {e}") from e
        seeds = [f"{n.name}:26257" for n in topo.nodes[:3]]
        for region in topo.regions:
            self._ops.step(op, f"Allocating capacity in {region.name}")
            for node in topo.nodes:
                if node.region == region.name:
                    self._docker.run_node(node, topo, seeds, self._settings.network)
        self._ops.step(op, "Forming the cluster")
        self._init(topo.nodes[0], topo.cluster_id)
        self._ops.step(op, "Configuring the multi-region database")
        for warning in self._bootstrap(topo):
            self._ops.step(op, f"Note: {warning}")
        self._ops.step(op, "Configuring the global load balancer")
        self._lb.apply(topo.nodes)
        save_state(self._settings.data_dir, {"topology": topology_to_dict(topo), "provisioned": True})
        self._ready(op)
        self._ops.step(op, "Cluster ready")

    def _init(self, node: NodeSpec, cluster_id: str) -> None:
        cmd = ["/cockroach/cockroach", "init", "--insecure", f"--host={node.name}:26257", f"--cluster-name={cluster_id}"]
        last = ""
        for _ in range(INIT_ATTEMPTS):
            proc = self._docker.exec(node.name, cmd, timeout=30, check=False)
            output = f"{proc.stdout}{proc.stderr}"
            if proc.returncode == 0 or "already been initialized" in output:
                return
            last = output.strip()
            self._sleep(1)
        raise ProvisionError(f"The cluster did not initialize: {last}")

    def _sql(self, host: str):
        last: Exception | None = None
        for _ in range(SQL_ATTEMPTS):
            try:
                return self._connect(host)
            except Exception as e:  # node still starting
                last = e
                self._sleep(1)
        raise ProvisionError(f"SQL never became available on {host}: {last}")

    def _bootstrap(self, topo: Topology) -> list[str]:
        conn = self._sql(topo.nodes[0].ip)
        try:
            wanted = {r.name for r in topo.regions}
            have: set[str] = set()
            for _ in range(REGION_ATTEMPTS):
                with conn.cursor() as cur:
                    cur.execute("SELECT region FROM [SHOW REGIONS FROM CLUSTER]")
                    have = {row[0] for row in cur.fetchall()}
                if wanted <= have:
                    break
                self._sleep(1)
            else:
                missing = ", ".join(sorted(wanted - have))
                raise ProvisionError(f"These regions never joined the cluster: {missing}")
            statements = bootstrap_statements(topo, time_until_store_dead=self._settings.time_until_store_dead,
                                              license=self._settings.crdb_license, org=self._settings.crdb_org)
            return run_bootstrap(conn, statements, log=log.warning)
        finally:
            conn.close()
