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


def test_sink_measures_the_longest_pause_in_the_event_stream_since_a_mark():
    """Resolved timestamps can be 30 s apart on a multi-region cluster, so they can't time a pause. The rows can:
    under load they arrive many times a second, and a gap between them is the stream standing still."""
    now = [1000.0]
    sink = CdcSink(clock=lambda: now[0])
    row = {"payload": [{"key": ["a"], "after": {"balance": 1}}]}
    assert sink.status()["idle_s"] is None and sink.status()["max_pause_s"] == 0.0
    sink.ingest(row)
    now[0] += 20; sink.ingest(row)                 # a 20 s gap before anyone was measuring
    sink.mark()                                    # the node running the feed is crashed here
    assert sink.status()["max_pause_s"] == 0.0
    now[0] += 0.5; sink.ingest(row)
    now[0] += 7.0                                  # nothing for 7 s: the pause counts while it is still going on
    assert sink.status()["idle_s"] == 7.0 and sink.status()["max_pause_s"] == 7.0
    now[0] += 5.0; sink.ingest(row)                # 12 s in all, then rows again
    now[0] += 0.2; sink.ingest(row)
    st = sink.status()
    assert st["max_pause_s"] == 12.0 and st["idle_s"] == 0.0
    sink.ingest({"resolved": "1760054400000000000.0000000000"})   # a checkpoint is not a row: it doesn't end a pause
    assert sink.status()["max_pause_s"] == 12.0
