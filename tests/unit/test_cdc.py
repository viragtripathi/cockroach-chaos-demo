from controlplane.cdc import CdcSink, hlc_seconds


def test_hlc_parsing():
    assert hlc_seconds("1760054400000000000.0000000001") == 1760054400.0
    assert hlc_seconds("garbage") is None


def test_sink_counts_rows_and_tracks_resolved_gaps():
    now = [100.0]
    sink = CdcSink(clock=lambda: now[0])
    sink.ingest({"payload": [{"key": [1], "after": {"balance": 5}}, {"key": [2], "after": {"balance": 7}}], "length": 2})
    sink.ingest({"resolved": f"{int(99e9)}.0"})
    now[0] = 112.0
    sink.ingest({"resolved": f"{int(111e9)}.0"})
    st = sink.status()
    assert st["events"] == 2 and st["max_gap_s"] == 12.0 and st["resolved_lag_s"] == 1.0
    assert st["recent"][-1] == {"key": [2], "balance": 7}
