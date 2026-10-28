"""Sixteen LangGraph agents on CockroachDB while you take nodes or a region down.

Run it against the chaos demo (./run.sh up) and watch the agents keep working
through a failure. Each simulated user runs agent turns in a loop: the model
calls a search tool backed by the CockroachDB vector store, then answers, and
the LangGraph checkpointer saves every step. The model and the embeddings are
stubbed, so the run needs no API key and costs nothing. Every database call is
real, and this script never retries anything itself; langchain-cockroachdb does.

    python agents/agent_demo.py     # take a node or a region down after READY

The cluster needs a license key: put it in CRDB_LICENSE in .env and run
./run.sh up, which applies it to the running cluster.

When the cluster has recovered, it checks every conversation for lost or
duplicated messages and prints a RESULT line. Details go to agents/run/.

Optional settings:
    DEMO_SMOKE=1        a 25 second run with no fault, to check the setup
    DEMO_FRESH=1        drop the agent_demo database first
    CHAOS_SQL_URL       where to connect (default postgresql://root@localhost:26257)
    CHAOS_CONTROL_URL   the control plane (default http://localhost:8088)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import statistics
import threading
import time
import urllib.request
import uuid
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import psycopg
from langchain.agents import create_agent
from langchain_cockroachdb import (
    AsyncCockroachDBSaver,
    AsyncCockroachDBVectorStore,
    CockroachDBEngine,
    RetryPolicy,
)
from langchain_core.embeddings import DeterministicFakeEmbedding
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import tool
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

DB = "agent_demo"
SQL = os.getenv("CHAOS_SQL_URL", "postgresql://root@localhost:26257").rstrip("/")
ADMIN_URL = f"{SQL}/defaultdb?sslmode=disable"
URL = f"{SQL}/{DB}?sslmode=disable&application_name=agent_demo"
CONTROL = os.getenv("CHAOS_CONTROL_URL", "http://localhost:8088").rstrip("/")
RUN = Path(__file__).parent / "run"
RUN_ID = uuid.uuid4().hex[:6]
SMOKE = os.getenv("DEMO_SMOKE") == "1"
WARMUP_S = 30
AFTER_RECOVERY_S = 40
MAX_S = 25 if SMOKE else 15 * 60

POLICY = RetryPolicy(max_attempts=8, attempt_timeout=10)

# CockroachDB's error code when it limits open transactions for the cluster's license
LICENSE_SQLSTATE = "XXC02"
LICENSE_HINT = (
    "CockroachDB is limiting open transactions because of the cluster's license. "
    "Put a valid key in CRDB_LICENSE in .env, run ./run.sh up so the control plane "
    "applies it (editing .env alone does not), then run this script again"
)

USERS = [
    "alice",
    "bob",
    "chen",
    "dana",
    "eli",
    "fatima",
    "gus",
    "hana",
    "ivan",
    "jo",
    "kai",
    "lena",
    "mo",
    "nia",
    "omar",
    "priya",
]
QUESTIONS = [
    "Who approves refunds over $500?",
    "When does the on-call rotation change?",
    "How long do we keep audit logs?",
    "What's the SLA for priority one tickets?",
    "Where is the incident runbook?",
    "Which regions serve EU customers?",
    "How do I rotate an API key?",
    "What did we decide about the Q4 launch?",
]
DOCS = [
    "Refunds over $500 need approval from a support manager.",
    "The on-call rotation changes every Monday at 09:00 UTC.",
    "Audit logs are kept for 400 days, then archived.",
    "Priority one tickets get a first response within 15 minutes.",
    "The incident runbook lives in the ops handbook, section 4.",
    "EU customers are served from eu-west-1 and stay in the EU.",
    "API keys are rotated from the settings page, under Security.",
    "The Q4 launch moved to November 12 after the design review.",
    "Escalations go to the duty manager after 30 minutes.",
    "Chargebacks are handled by the payments team, not support.",
    "Customer data exports run nightly at 02:00 UTC.",
    "New hires get production access after two weeks.",
]

_lock = threading.Lock()
_log_file = None
CLUSTER: dict[str, Any] = {
    "fault_at": None,
    "recovered_at": None,
    "affected": [],
    "license_failures": 0,
}


def log_event(kind: str, **fields: Any) -> None:
    record = {"ts": time.time(), "kind": kind, **fields}
    with _lock:
        _log_file.write(json.dumps(record) + "\n")
        _log_file.flush()


def say(text: str) -> None:
    print(f"{time.strftime('%H:%M:%S')}  {text}", flush=True)


def license_error(exc: BaseException) -> bool:
    """Whether CockroachDB refused the work because of the cluster's license."""
    error: BaseException | None = exc
    while error is not None:
        if getattr(error, "sqlstate", None) == LICENSE_SQLSTATE:
            return True
        # The vector store's errors come from SQLAlchemy, which keeps psycopg's in .orig
        error = getattr(error, "orig", None) or error.__cause__
    return False


class LibraryLog(logging.Handler):
    """Records the library's own WARNING lines; each "Retrying" line is a retry."""

    def emit(self, record: logging.LogRecord) -> None:
        msg = record.getMessage()
        log_event(
            "retry" if msg.startswith("Retrying") else "warning",
            msg=msg,
            logger=record.name,
        )
        say(f"{record.levelname} {msg}")


class ScriptedModel(BaseChatModel):
    """Stands in for an LLM: asks the search tool first, then answers."""

    @property
    def _llm_type(self) -> str:
        return "scripted"

    def bind_tools(self, tools: Any, **kwargs: Any) -> ScriptedModel:
        return self

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: Any = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        last = messages[-1]
        if isinstance(last, HumanMessage):
            call = {
                "name": "search_docs",
                "args": {"query": last.content},
                "id": f"call_{uuid.uuid4().hex[:12]}",
                "type": "tool_call",
            }
            message = AIMessage(content="", tool_calls=[call])
        else:
            message = AIMessage(
                content=f"Here's what I found: {str(last.content)[:80]}"
            )
        return ChatResult(generations=[ChatGeneration(message=message)])

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: Any = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        return self._generate(messages, stop)


def watch_cluster(stop: threading.Event) -> None:
    """Follow the control plane's state stream and log every change."""
    last = None
    while not stop.is_set():
        try:
            with urllib.request.urlopen(f"{CONTROL}/api/events", timeout=30) as resp:
                for raw in resp:
                    if stop.is_set():
                        return
                    line = raw.decode().strip()
                    if not line.startswith("data:"):
                        continue
                    state = json.loads(line[5:])
                    cluster = state["cluster"]
                    affected = sorted(
                        n["name"]
                        for n in state["nodes"]
                        if n["runtime"] != "running"
                        or n["liveness"] != "live"
                        or n.get("partitioned")
                        or n.get("isolated")
                        or n.get("brownout")
                    )
                    regions = {r["name"]: r["state"] for r in state["regions"]}
                    traffic = state.get("traffic", {}).get("cluster", {})
                    log_event(
                        "traffic",
                        **{
                            k: traffic.get(k)
                            for k in (
                                "txn_s",
                                "stmt_s",
                                "p99_ms",
                                "retries_s",
                                "errors_s",
                                "conns",
                            )
                        },
                    )
                    snap = (
                        cluster["live_nodes"],
                        cluster["status"],
                        tuple(affected),
                        state["ranges"]["unavailable"],
                        tuple(sorted(regions.items())),
                    )
                    if snap != last:
                        last = snap
                        log_event(
                            "cluster",
                            live=cluster["live_nodes"],
                            total=cluster["total_nodes"],
                            status=cluster["status"],
                            affected=affected,
                            unavailable=state["ranges"]["unavailable"],
                            regions=regions,
                        )
                        say(
                            f"cluster {cluster['status']} {cluster['live_nodes']}/{cluster['total_nodes']} affected={affected} unavailable={state['ranges']['unavailable']}"
                        )
                    now = time.time()
                    if affected and CLUSTER["fault_at"] is None:
                        CLUSTER["fault_at"], CLUSTER["affected"] = now, affected
                        log_event("fault", affected=affected)
                        say(f"FAULT seen: {affected}")
                    if (
                        CLUSTER["fault_at"]
                        and not CLUSTER["recovered_at"]
                        and not affected
                        and cluster["live_nodes"] == cluster["total_nodes"]
                    ):
                        CLUSTER["recovered_at"] = now
                        log_event("recovered")
                        say("RECOVERED: every node is live again")
                    for item in state.get("events") or []:
                        key = json.dumps(item, sort_keys=True)
                        if key not in watch_cluster.seen:
                            watch_cluster.seen.add(key)
                            log_event("feed", item=item)
        except Exception as exc:  # noqa: BLE001  # the stream can drop while nodes restart
            log_event("watch_error", error=str(exc)[:200])
            time.sleep(1)


watch_cluster.seen = set()


async def user_loop(
    name: str,
    agent: Any,
    ok: dict[str, int],
    failed: list[dict[str, Any]],
    stop: asyncio.Event,
) -> None:
    config = {"configurable": {"thread_id": f"{RUN_ID}-{name}"}}
    turn = 0
    await asyncio.sleep(random.uniform(0, 1.5))
    while not stop.is_set():
        turn += 1
        question = random.choice(QUESTIONS)
        started = time.perf_counter()
        try:
            # The question's id names the turn, so the final check can find its messages
            await agent.ainvoke(
                {"messages": [HumanMessage(question, id=f"{name}-{turn}")]}, config
            )
        except Exception as exc:  # noqa: BLE001  # record any failed turn and keep going
            ms = round((time.perf_counter() - started) * 1000)
            error = f"{type(exc).__name__}: {exc}"[:300]
            failed.append({"user": name, "turn": turn, "error": error})
            log_event(
                "turn",
                user=name,
                turn=turn,
                question=question,
                ms=ms,
                ok=False,
                error=error,
            )
            if not license_error(exc):
                say(f"FAILED {name} turn {turn}: {error}")
            else:
                # Explain once, instead of a line for every turn the license stops
                CLUSTER["license_failures"] += 1
                if CLUSTER["license_failures"] == 1:
                    say(f"FAILED {name} turn {turn}: {error}")
                    say(f"LICENSE: {LICENSE_HINT}")
        else:
            ms = round((time.perf_counter() - started) * 1000)
            ok[name] += 1
            log_event("turn", user=name, turn=turn, question=question, ms=ms, ok=True)
        await asyncio.sleep(random.uniform(0.4, 1.2))


async def controller(
    stop: asyncio.Event, ok: dict[str, int], failed: list[dict[str, Any]]
) -> None:
    started = time.time()
    announced = False
    last_print = 0.0
    while True:
        await asyncio.sleep(0.5)
        now = time.time()
        if not announced and now - started > WARMUP_S and not SMOKE:
            announced = True
            log_event("ready")
            say("READY: steady state recorded, inject the fault whenever you like")
        if now - last_print > 10:
            last_print = now
            unlicensed = CLUSTER["license_failures"]
            note = f" ({unlicensed} stopped by the license limit)" if unlicensed else ""
            say(f"turns ok={sum(ok.values())} failed={len(failed)}{note}")
        if CLUSTER["recovered_at"] and now - CLUSTER["recovered_at"] > AFTER_RECOVERY_S:
            break
        if now - started > MAX_S:
            log_event("timeout")
            break
        if (RUN / "STOP").exists():
            break
    stop.set()


async def main() -> None:
    global _log_file
    RUN.mkdir(exist_ok=True)
    (RUN / "STOP").unlink(missing_ok=True)  # touch agents/run/STOP to end a run early
    _log_file = (RUN / "events.jsonl").open("w")
    library = logging.getLogger("langchain_cockroachdb")
    library.setLevel(logging.WARNING)
    library.addHandler(LibraryLog())
    library.propagate = False

    # On a multi-region cluster with realistic latency every schema change waits on
    # cross-region round trips, so the first start takes a few minutes. Later starts
    # reuse the tables.
    say(f"setting up the {DB} database; the first start can take a few minutes")
    with psycopg.connect(ADMIN_URL, autocommit=True) as conn:
        if os.getenv("DEMO_FRESH") == "1":
            conn.execute(f"DROP DATABASE IF EXISTS {DB} CASCADE")
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {DB}")

    say("loading the knowledge base into the vector store")
    engine = CockroachDBEngine.from_connection_string(
        URL, retry_policy=POLICY, pool_size=8
    )
    await engine.ainit_vectorstore_table("kb", vector_dimension=256)
    kb = AsyncCockroachDBVectorStore(engine, DeterministicFakeEmbedding(size=256), "kb")
    await kb.aadd_texts(
        DOCS, ids=[str(uuid.uuid5(uuid.NAMESPACE_URL, d)) for d in DOCS]
    )

    @tool
    async def search_docs(query: str) -> str:
        """Search the team knowledge base."""
        docs = await kb.asimilarity_search(query, k=3)
        return " | ".join(d.page_content for d in docs)

    pool = AsyncConnectionPool(
        URL,
        kwargs={"autocommit": True, "row_factory": dict_row, "connect_timeout": 10},
        min_size=8,
        max_size=20,
        open=False,
    )
    await pool.open(wait=True)
    # One saver on a pool serves every agent: since langchain-cockroachdb 0.4.1 a
    # pooled saver runs its operations concurrently.
    say("setting up the checkpointer")
    checkpointer = AsyncCockroachDBSaver(pool, retry_policy=POLICY)
    await checkpointer.setup()
    agents = {
        name: create_agent(
            ScriptedModel(), tools=[search_docs], checkpointer=checkpointer
        )
        for name in USERS
    }

    stop_watch = threading.Event()
    watcher = threading.Thread(target=watch_cluster, args=(stop_watch,), daemon=True)
    watcher.start()

    ok: dict[str, int] = defaultdict(int)
    failed: list[dict[str, Any]] = []
    stop = asyncio.Event()
    log_event(
        "start",
        run_id=RUN_ID,
        users=USERS,
        policy={
            "max_attempts": POLICY.max_attempts,
            "attempt_timeout": POLICY.attempt_timeout,
        },
    )
    say(
        f"run {RUN_ID}: {len(USERS)} users, policy max_attempts={POLICY.max_attempts} attempt_timeout={POLICY.attempt_timeout}"
    )
    await asyncio.gather(
        controller(stop, ok, failed),
        *(user_loop(u, agents[u], ok, failed, stop) for u in USERS),
    )
    stop_watch.set()

    # Every successful turn adds exactly four messages: question, tool call, tool
    # result, answer. A failed turn can leave the first few behind, as a partial turn.
    per_user = {}
    for name in USERS:
        state = await agents[name].aget_state(
            {"configurable": {"thread_id": f"{RUN_ID}-{name}"}}
        )
        messages = state.values.get("messages", [])
        # Each turn's messages follow its question
        saved: dict[str | None, int] = defaultdict(int)
        turn_id = None
        for m in messages:
            if isinstance(m, HumanMessage):
                turn_id = m.id
            saved[turn_id] += 1
        failed_ids = {f"{name}-{f['turn']}" for f in failed if f["user"] == name}
        turn_ids = {f"{name}-{t}" for t in range(1, ok[name] + len(failed_ids) + 1)}
        # A turn accounts for up to four messages; anything more is extra
        counted = {t: min(saved.get(t, 0), 4) for t in turn_ids}
        per_user[name] = {
            "turns_ok": ok[name],
            "turns_failed": len(failed_ids),
            "messages": len(messages),
            "unique_ids": len({m.id for m in messages}),
            "lost": sum(4 - counted[t] for t in turn_ids - failed_ids),
            "extra": len(messages) - sum(counted.values()),
            "partial": sum(counted[t] for t in failed_ids),
        }
    lat = [
        json.loads(line)["ms"]
        for line in (RUN / "events.jsonl").read_text().splitlines()
        if '"kind": "turn"' in line and '"ok": true' in line
    ]
    retries = sum(
        1
        for line in (RUN / "events.jsonl").read_text().splitlines()
        if '"kind": "retry"' in line
    )
    summary = {
        "turns_ok": sum(ok.values()),
        "turns_failed": len(failed),
        "license_failures": CLUSTER["license_failures"],
        # Each distinct error's first line, with how many turns it failed
        "failures": Counter(f["error"].splitlines()[0] for f in failed).most_common(10),
        "retries": retries,
        "lost_messages": sum(u["lost"] for u in per_user.values()),
        "duplicate_messages": sum(
            u["messages"] - u["unique_ids"] for u in per_user.values()
        ),
        "extra_messages": sum(u["extra"] for u in per_user.values()),
        "partial_turn_messages": sum(u["partial"] for u in per_user.values()),
        "p50_ms": round(statistics.median(lat)) if lat else None,
        "p99_ms": round(statistics.quantiles(lat, n=100)[98])
        if len(lat) >= 100
        else None,
        "max_ms": max(lat) if lat else None,
        "per_user": per_user,
        "fault_at": CLUSTER["fault_at"],
        "recovered_at": CLUSTER["recovered_at"],
        "affected": CLUSTER["affected"],
    }
    log_event("summary", **summary)
    (RUN / "summary.json").write_text(json.dumps(summary, indent=2))
    say("SUMMARY " + json.dumps({k: v for k, v in summary.items() if k != "per_user"}))
    # Messages a failed turn saved before it failed are not lost or duplicated
    wrong = {
        "lost": summary["lost_messages"],
        "duplicated": summary["duplicate_messages"],
        "extra": summary["extra_messages"],
    }
    outage = ""
    if CLUSTER["fault_at"] and CLUSTER["recovered_at"]:
        seconds = CLUSTER["recovered_at"] - CLUSTER["fault_at"]
        outage = (
            f" through a {seconds:.0f} s outage of {len(CLUSTER['affected'])} node(s)"
        )
    if summary["turns_failed"] + sum(wrong.values()) == 0:
        say(
            f"RESULT: {summary['turns_ok']} agent turns{outage}, none failed, no lost or "
            f"duplicated messages; the library retried {summary['retries']} times"
        )
    else:
        found = ", ".join(f"{n} {kind}" for kind, n in wrong.items() if n)
        messages = (
            f"messages: {found}" if found else "no lost, duplicated or extra messages"
        )
        if summary["partial_turn_messages"]:
            messages += (
                f"; the failed turns left {summary['partial_turn_messages']} "
                "messages behind as partial turns"
            )
        turns = summary["turns_ok"] + summary["turns_failed"]
        say(
            f"RESULT: {summary['turns_failed']} of {turns} agent turns failed{outage}; "
            f"{messages}; see {RUN / 'summary.json'}"
        )
    if summary["license_failures"]:
        say(
            f"LICENSE: {summary['license_failures']} turns hit the license limit. "
            f"{LICENSE_HINT}"
        )
    await pool.close()
    await engine.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        # The license can stop the setup too; say why instead of a traceback
        if not license_error(exc):
            raise
        say(f"STOPPED: {type(exc).__name__}: {exc}"[:300])
        say(f"LICENSE: {LICENSE_HINT}")
        raise SystemExit(1) from None
