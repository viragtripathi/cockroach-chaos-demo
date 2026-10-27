from controlplane.metrics import Metrics, histogram_quantile, parse_vars, rate

VARS_1 = """# HELP sql_query_count x
sql_query_count{node_id="1"} 100
sql_txn_latency_count{node_id="1"} 50
sql_txn_abort_count{node_id="1"} 1
sql_failure_count{node_id="1"} 2
txn_restarts_sum{node_id="1"} 3
sql_conns{node_id="1"} 16
sql_service_latency_bucket{node_id="1",le="1e+06"} 10
sql_service_latency_bucket{node_id="1",le="1e+07"} 90
sql_service_latency_bucket{node_id="1",le="1e+08"} 100
sql_service_latency_bucket{node_id="1",le="+Inf"} 100
"""
VARS_2 = VARS_1.replace("} 100\nsql_txn", "} 300\nsql_txn").replace('sql_txn_latency_count{node_id="1"} 50', 'sql_txn_latency_count{node_id="1"} 250') \
    .replace('le="1e+06"} 10', 'le="1e+06"} 10').replace('le="1e+07"} 90', 'le="1e+07"} 280') \
    .replace('le="1e+08"} 100', 'le="1e+08"} 300').replace('le="+Inf"} 100', 'le="+Inf"} 300')


def test_parse_vars_reads_scalars_and_buckets():
    scalars, buckets = parse_vars(VARS_1)
    assert scalars["sql_query_count"] == 100 and scalars["sql_conns"] == 16
    assert buckets[0] == (1e6, 10) and buckets[-1] == (float("inf"), 100)


def test_rates_treat_counter_resets_as_zero():
    assert rate(100, 300, 2.0) == 100
    assert rate(300, 5, 2.0) == 0


def test_p99_from_bucket_deltas_in_ms():
    _, b1 = parse_vars(VARS_1)
    _, b2 = parse_vars(VARS_2)
    # 200 new observations: 0 under 1ms, 190 under 10ms, 200 under 100ms -> p99 falls in the 10-100 ms bucket
    p99 = histogram_quantile(0.99, b1, b2)
    assert 10 < p99 <= 100
    assert histogram_quantile(0.99, b1, b1) is None


def test_metrics_aggregate_per_region_and_keep_history():
    pages = {"10.0.1.11": VARS_1, "10.0.2.11": VARS_1}
    now = [0.0]
    m = Metrics(fetch=lambda ip: pages.get(ip), clock=lambda: now[0])
    nodes = [("a", "r1", "10.0.1.11"), ("b", "r2", "10.0.2.11"), ("c", "r2", "10.0.2.12")]
    m.poll(nodes)
    pages = {"10.0.1.11": VARS_2, "10.0.2.11": VARS_2}
    m._fetch = lambda ip: pages.get(ip)
    now[0] = 2.0
    m.poll(nodes)
    out = m.status()
    assert out["cluster"]["txn_s"] == 200 and out["regions"]["r1"]["txn_s"] == 100
    assert out["cluster"]["retries_s"] == 0 and out["cluster"]["conns"] == 32
    assert out["nodes"]["c"] is None and out["history"][-1]["regions"] == {"r1": 100, "r2": 100}


def test_cluster_reports_p50_p95_and_p99():
    pages = {"10.0.1.11": VARS_1}
    now = [0.0]
    m = Metrics(fetch=lambda ip: pages.get(ip), clock=lambda: now[0])
    m.poll([("a", "r1", "10.0.1.11")])
    pages["10.0.1.11"], now[0] = VARS_2, 2.0
    m.poll([("a", "r1", "10.0.1.11")])
    c = m.status()["cluster"]
    assert c["p50_ms"] is not None and c["p50_ms"] <= c["p95_ms"] <= c["p99_ms"]
