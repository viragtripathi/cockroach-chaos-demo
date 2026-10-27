import psycopg
import pytest

from controlplane.retry import classify, is_retryable, may_have_committed, retry_with_backoff


class Coded(Exception):
    def __init__(self, code, msg="boom", attr="sqlstate"):
        super().__init__(msg)
        setattr(self, attr, code)


@pytest.mark.parametrize("code, kind", [
    ("40001", "serialization"),
    ("40003", "ambiguous"),
    ("08000", "connection"), ("08001", "connection"), ("08003", "connection"), ("08004", "connection"),
    ("08006", "connection"), ("08007", "connection"), ("08S01", "connection"),
    ("57P01", "connection"),
    ("57014", "timeout"),
    ("23505", "duplicate"),
    ("42601", "other"),
])
def test_sqlstates_from_the_jdbc_wrapper_and_more(code, kind):
    assert classify(Coded(code)) == kind
    assert classify(Coded(code, attr="pgcode")) == kind  # psycopg2-style attribute works too


@pytest.mark.parametrize("message, kind", [
    ("TransactionRetryWithProtoRefreshError: restart transaction", "serialization"),
    ("could not serialize access due to read/write dependencies (serialization failure)", "serialization"),
    ("server closed the connection unexpectedly", "connection"),
    ("connection reset by peer", "connection"),
    ("broken pipe", "connection"),
    ("sorry, too many clients already", "connection"),
    ("the connection is closed", "connection"),
    ("query_wait timeout", "timeout"),
    # banko-ai-assistant db_retry.py patterns
    ("could not receive data from server", "connection"),
    ("failed to connect to `host=haproxy`", "connection"),
    ("dial tcp: lookup us-east-1a-n1: no such host", "connection"),
    ("sending to all replicas failed", "connection"),
    ("initial connection heartbeat failed", "connection"),
    ("SSL connection has been closed unexpectedly", "connection"),
    ("StatementCompletionUnknown: result is ambiguous", "ambiguous"),
    ("TransactionRetryError: retry txn", "serialization"),
    ("psycopg2.errors.SerializationFailure", "serialization"),
    ("ERROR 40001 something", "serialization"),
    ("ERROR 40003 something", "ambiguous"),
    ("syntax error at or near", "other"),
])
def test_message_patterns_from_langchain_catch_errors_without_a_code(message, kind):
    assert classify(Exception(message)) == kind


def test_driver_connection_errors_without_a_code_are_connection_errors():
    assert classify(psycopg.OperationalError("whatever")) == "connection"
    assert classify(psycopg.InterfaceError("whatever")) == "connection"


def test_retryable_and_maybe_committed():
    for code in ("40001", "40003", "08006", "57P01", "57014"):
        assert is_retryable(Coded(code))
    assert not is_retryable(Coded("23505")) and not is_retryable(Coded("42601"))
    assert not may_have_committed(Coded("40001"))  # CockroachDB guarantees it did not commit
    assert may_have_committed(Coded("40003")) and may_have_committed(Coded("08006"))


def test_backoff_retries_with_growing_jittered_delays_then_succeeds():
    calls, sleeps = [], []

    def flaky():
        calls.append(1)
        if len(calls) < 4:
            raise Coded("40001")
        return "ok"
    assert retry_with_backoff(flaky, max_retries=5, initial_backoff=0.1, sleep=sleeps.append) == "ok"
    assert len(sleeps) == 3
    for i, s in enumerate(sleeps):  # 50-100% of 0.1, 0.2, 0.4
        assert 0.05 * 2 ** i <= s <= 0.1 * 2 ** i


def test_backoff_gives_up_after_max_retries_and_never_retries_real_errors():
    sleeps = []
    with pytest.raises(Coded):
        retry_with_backoff(lambda: (_ for _ in ()).throw(Coded("08006")), max_retries=3, sleep=sleeps.append)
    assert len(sleeps) == 2
    sleeps.clear()
    with pytest.raises(Coded):
        retry_with_backoff(lambda: (_ for _ in ()).throw(Coded("42601")), sleep=sleeps.append)
    assert sleeps == []


def test_backoff_is_capped():
    sleeps = []
    with pytest.raises(Coded):
        retry_with_backoff(lambda: (_ for _ in ()).throw(Coded("40001")), max_retries=8, initial_backoff=1,
                           max_backoff=2, jitter=False, sleep=sleeps.append)
    assert sleeps == [1, 2, 2, 2, 2, 2, 2]


def test_on_retry_hook_and_custom_predicate():
    seen = []
    with pytest.raises(Coded):
        retry_with_backoff(lambda: (_ for _ in ()).throw(Coded("40003")), max_retries=3, sleep=lambda s: None,
                           retry_if=lambda e: classify(e) == "serialization", on_retry=lambda e, n: seen.append(n))
    assert seen == []  # ambiguous results are not retried when the caller says the work isn't idempotent


def test_abandon_after_unblocks_a_call_stuck_on_its_socket():
    import socket
    import time
    from controlplane.retry import abandon_after

    a, b = socket.socketpair()

    class Conn:
        def fileno(self):
            return a.fileno()

    started = time.monotonic()
    with abandon_after(Conn(), 0.2) as fired:
        data = a.recv(10)  # would block forever: nothing is ever sent on b
    assert data == b"" and fired.is_set() and time.monotonic() - started < 2
    b.close()
    a.close()


def test_abandon_after_does_nothing_when_the_work_finishes_in_time():
    import socket
    import time
    from controlplane.retry import abandon_after

    a, b = socket.socketpair()

    class Conn:
        def fileno(self):
            return a.fileno()

    with abandon_after(Conn(), 0.3) as fired:
        pass
    time.sleep(0.5)
    b.send(b"x")
    assert a.recv(1) == b"x" and not fired.is_set()
    a.close()
    b.close()
