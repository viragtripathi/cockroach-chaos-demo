# LangGraph agents through a failure

`agent_demo.py` runs 16 AI agents on this cluster with
[langchain-cockroachdb](https://github.com/cockroachdb/langchain-cockroachdb).
Each agent answers questions by searching a CockroachDB vector store, and the
LangGraph checkpointer saves every step of every conversation. You take part of
the cluster down while they run, and the script shows that no conversation lost
or duplicated a message. The model and the embeddings are scripted, so it needs
no API key and costs nothing; every database call is real.

## Run it

The cluster needs a license key. Without one, CockroachDB throttles a multi-node
cluster to 5 open transactions 7 days after it was built, and most agent turns
fail with `No license installed`. Put the key in `CRDB_LICENSE` in `.env`, then
run `./run.sh up`: the control plane applies it to the running cluster when it
starts. Editing `.env` alone changes nothing.

```bash
./run.sh up                                    # in this repo, if the cluster is not running yet
python3 -m venv agents/.venv && source agents/.venv/bin/activate
pip install -r agents/requirements.txt
python agents/agent_demo.py
```

1. Wait for `READY`, after about 30 seconds of steady traffic. The script logs
   completed and failed turns every 10 seconds. The first start takes a few
   minutes on a multi-region cluster with realistic latency, while it creates its
   tables; later starts reuse them.
2. Take something down from the presenter console at http://localhost:8088/presenter:
   a node, a zone, or, in the default multi-region layout, a whole region.
3. Bring it back. Forty seconds after the cluster has recovered, the script
   stops and prints a `RESULT` line: how many agent turns ran, how long the
   outage lasted, whether any turn failed or any message was lost or
   duplicated, and how many times the library retried. A turn that fails can
   leave its first messages saved; the script counts those as partial turns,
   not as lost or duplicated messages.

During the outage, turns slow down while CockroachDB moves leases away from the
failed nodes. The connector retries the statements the failure interrupted, so
the agents' turns complete instead of failing. The script itself never retries.

## What it writes

- `agents/run/summary.json`: turns and messages per user, the errors that
  failed turns, latency percentiles, and when the fault and the recovery
  happened.
- `agents/run/events.jsonl`: every turn, retry and cluster change, one per line.

## Options

| Setting | Does |
|---|---|
| `DEMO_SMOKE=1` | A 25 second run with no fault, to check your setup |
| `DEMO_FRESH=1` | Drops the `agent_demo` database before starting |
| `CHAOS_SQL_URL` | Where to connect; default `postgresql://root@localhost:26257` |
| `CHAOS_CONTROL_URL` | The control plane; default `http://localhost:8088` |

To end a run early, `touch agents/run/STOP`.

## How it works

Sixteen simulated users each keep their own conversation thread. On every turn
a scripted model calls a `search_docs` tool, backed by
`AsyncCockroachDBVectorStore`, then answers with what it found. One
`AsyncCockroachDBSaver` on a connection pool saves every step for all of them.
Both use `RetryPolicy(max_attempts=8, attempt_timeout=10)`, so a call the
server never answers is cut off after 10 seconds and retried. The script uses
its own `agent_demo` database, so it never touches Banko's `defaultdb` or the
Payments workload's `chaos` database.
