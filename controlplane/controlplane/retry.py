"""Transaction retry for CockroachDB: which errors are transient, and exponential backoff with jitter.

Covers the SQLSTATEs retried by cockroachdb-jdbc-wrapper (40001, 40003, 08xxx, 57P01) plus statement timeouts,
and the message patterns from langchain-cockroachdb and banko-ai-assistant for errors that arrive without a code.

40001 guarantees the transaction did not commit, so retrying is always safe. After 40003 (ambiguous result), a
dropped connection or a timeout the transaction MAY have committed: retry only work that is idempotent, for
example by writing a unique id in the same transaction and treating a duplicate (23505) as "already applied".
"""
from __future__ import annotations

import os
import random
import socket
import threading
import time
from contextlib import contextmanager
from typing import Callable, TypeVar

import psycopg

T = TypeVar("T")

RETRYABLE = frozenset({"serialization", "ambiguous", "connection", "timeout"})
MAYBE_COMMITTED = frozenset({"ambiguous", "connection", "timeout"})

_PATTERNS = (  # checked in order, lowercase; from langchain-cockroachdb retry.py and banko-ai-assistant db_retry.py
    ("ambiguous", ("result is ambiguous", "statementcompletionunknown", "40003")),
    ("serialization", ("restart transaction", "serialization failure", "serializationfailure", "could not serialize",
                       "transactionretryerror", "40001")),
    ("timeout", ("query_wait", "timeout", "timed out")),
    ("connection", ("connection", "closed", "broken pipe", "too many clients", "server closed", "reset by peer",
                    "could not receive data from server", "failed to connect", "no such host",
                    "sending to all replicas failed")),
)


def sqlstate(error: BaseException) -> str | None:
    """psycopg 3 sets .sqlstate; psycopg2 sets .pgcode."""
    return getattr(error, "sqlstate", None) or getattr(error, "pgcode", None)


def classify(error: BaseException) -> str:
    """serialization | ambiguous | connection | timeout | duplicate | other"""
    code = sqlstate(error)
    if code == "40001":
        return "serialization"
    if code == "40003":
        return "ambiguous"
    if code and (code.startswith("08") or code == "57P01"):
        return "connection"
    if code == "57014":
        return "timeout"
    if code == "23505":
        return "duplicate"
    if code is None and isinstance(error, (psycopg.OperationalError, psycopg.InterfaceError)):
        return "connection"
    message = str(error).lower()
    for kind, patterns in _PATTERNS:
        if any(p in message for p in patterns):
            return kind
    return "other"


def is_retryable(error: BaseException) -> bool:
    return classify(error) in RETRYABLE


def may_have_committed(error: BaseException) -> bool:
    return classify(error) in MAYBE_COMMITTED


def backoff_delay(attempt: int, *, initial_backoff: float = 0.1, max_backoff: float = 10.0,
                  multiplier: float = 2.0, jitter: bool = True) -> float:
    """Delay before retry number `attempt` (0-based): exponential, capped, 50-100% jitter."""
    delay = min(initial_backoff * multiplier ** attempt, max_backoff)
    return delay * (0.5 + random.random() * 0.5) if jitter else delay


def retry_with_backoff(fn: Callable[[], T], *, max_retries: int = 5, initial_backoff: float = 0.1,
                       max_backoff: float = 10.0, multiplier: float = 2.0, jitter: bool = True,
                       retry_if: Callable[[BaseException], bool] = is_retryable,
                       on_retry: Callable[[BaseException, int], None] | None = None,
                       sleep: Callable[[float], None] = time.sleep) -> T:
    """Run fn, retrying transient failures up to max_retries attempts in total."""
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as e:
            if not retry_if(e) or attempt >= max_retries - 1:
                raise
            if on_retry:
                on_retry(e, attempt + 1)
            sleep(backoff_delay(attempt, initial_backoff=initial_backoff, max_backoff=max_backoff,
                                multiplier=multiplier, jitter=jitter))
    raise RuntimeError("unreachable")


@contextmanager
def abandon_after(conn, seconds: float):
    """Give up on a connection whose call hangs: after `seconds`, shut its socket so the blocked call fails with a
    connection error (retry it idempotently). Needed because CockroachDB's COMMIT ignores statement_timeout,
    transaction_timeout and cancel requests while a range it wrote to has lost quorum: the server blocks in a
    synchronous rollback (observed on v26.3)."""
    fired = threading.Event()

    def fire():
        fired.set()
        try:
            s = socket.socket(fileno=os.dup(conn.fileno()))
            s.shutdown(socket.SHUT_RDWR)
            s.close()
        except Exception:
            pass  # already closed, or not a socket-backed connection

    timer = threading.Timer(seconds, fire)
    timer.daemon = True
    timer.start()
    try:
        yield fired
    finally:
        timer.cancel()
