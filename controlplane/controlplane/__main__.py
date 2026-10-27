"""Entry point: wire the real collaborators, start background work, serve on :8088."""
from __future__ import annotations

import faulthandler
import logging
import signal
import threading
from functools import partial

import uvicorn

from .app import create_app, start_cluster
from .booth import Booth
from .cdc import CdcSink
from .catalog import load_catalog
from .chaos import Chaos
from .collector import Collector
from .docker_ops import Docker
from .elastic import Elastic
from .events import Events
from .lb import LoadBalancer
from .metrics import Metrics
from .netem import Netem
from .operations import Operations
from .provisioner import Provisioner
from .scenarios import Context, Runner
from .scenarios.library import ALL as SCENARIOS, CDC_PORT
from .settings import Settings
from .sqlconn import connect
from .store import Store
from .supervisor import Supervisor
from .topology import load_preset, subnet_prefix
from .workload import Workload

log = logging.getLogger("controlplane")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    faulthandler.register(signal.SIGUSR1, all_threads=True)  # `docker kill -s USR1 crdb-chaos-control-plane` dumps stacks
    settings = Settings.from_env()
    catalog = load_catalog(settings.catalog_path)
    docker = Docker()
    operations = Operations()
    events = Events()
    store = Store(settings.data_dir)
    lb = LoadBalancer(docker, settings.haproxy_cfg_dir, settings.haproxy_container)
    provisioner = Provisioner(docker, lb, operations, settings, connect=connect)
    netem = Netem(docker, prefix=subnet_prefix(settings.subnet), image=settings.helper_image)
    def busy() -> str | None:
        op = operations.snapshot()
        return op["kind"] if op and op["status"] == "running" else None

    chaos = Chaos(docker, provisioner, netem, store, network=settings.network, busy=busy)
    provisioner.on_ready.append(lambda: [log.warning(w) for w in chaos.apply_all()])
    provisioner.on_teardown.append(chaos.reset)
    elastic = Elastic(docker, provisioner, lb, operations, chaos, settings, connect=connect, catalog=catalog)
    booth = Booth(chaos, store, events, busy=busy)
    supervisor = Supervisor(docker, provisioner, chaos, events, busy=busy)
    workload = Workload(partial(connect, dbname="chaos", timeout=3, statement_timeout="5s"),
                        host=settings.haproxy_container, events=events)
    chaos.on_disrupt.append(workload.mark_failure)
    elastic.before_region_removal.append(workload.evacuate)
    elastic.after_region_removal.append(workload.resume)
    metrics = Metrics()
    cdc = CdcSink()
    try:
        cdc.serve(CDC_PORT, settings.tls_dir / "cert.pem", settings.tls_dir / "key.pem")
    except Exception as e:  # only the changefeed scenario needs it
        log.warning("changefeed sink disabled: %s", e)
        cdc = None
    scenario_ctx = Context(provisioner=provisioner, chaos=chaos, elastic=elastic, workload=workload,
                           operations=operations, events=events, connect=connect, lb_host=settings.haproxy_container,
                           catalog=catalog, cdc=cdc)
    scenarios = Runner(SCENARIOS, scenario_ctx)

    def load_topology(name: str, model: str | None = None):
        return load_preset(settings.presets_dir, name, catalog, default_version=settings.crdb_version,
                           subnet=settings.subnet, max_nodes=settings.max_nodes, model=model or settings.model)

    def system_info() -> dict:
        return {
            "sql_url": "postgresql://root@localhost:26257/defaultdb?sslmode=disable",
            "sql_url_in_network": "postgresql://root@haproxy:26257/defaultdb?sslmode=disable",
            "db_console": "http://localhost:8080",
            "haproxy_stats": "http://localhost:8404/stats",
            "stage": f"http://localhost:{settings.panel_port}/",
            "presenter": f"http://localhost:{settings.panel_port}/presenter",
            "network": settings.network,
            "subnet": settings.subnet,
            "crdb_version": settings.crdb_version,
            "license": "configured" if settings.crdb_license else
                       "none: CockroachDB throttles the cluster to 5 open transactions 7 days after it was built",
        }

    collector = Collector(docker, provisioner, operations, status_host=settings.haproxy_container, chaos=chaos,
                          network=settings.network,
                          extras=lambda: {"booth": booth.status(), "events": events.recent(12), "keys": store.get("keys"),
                                          "traffic": metrics.status(), "workload": workload.status(),
                                          "scenario": scenarios.status(), "cdc": cdc.status() if cdc else None})
    workload.cluster = collector.latest  # homed workers pick their gateway from the latest snapshot
    app = create_app(settings=settings, provisioner=provisioner, collector=collector, operations=operations,
                     load_topology=load_topology, chaos=chaos, elastic=elastic, booth=booth, store=store,
                     events=events, catalog=catalog, workload=workload, scenarios=scenarios, system_info=system_info)
    if not settings.control_token:
        log.warning("CONTROL_TOKEN is not set: anyone who can reach this panel can change the cluster")
    threading.Thread(target=start_cluster, args=(settings.preset,), daemon=True, name="startup",
                     kwargs=dict(load_topology=load_topology, provisioner=provisioner, operations=operations, lb=lb)).start()
    collector.start()
    booth.start()
    supervisor.start()
    metrics.start(lambda: [(n.name, n.region, n.ip) for n in (provisioner.topology.nodes if provisioner.topology else ())])
    uvicorn.run(app, host="0.0.0.0", port=8088, log_level="info")


if __name__ == "__main__":
    main()
