import psycopg
import pytest

from controlplane.sqlconn import INTERNALS_OPTION, connect


def test_internals_are_requested_on_every_connection():
    calls = []
    conn = connect("10.77.1.11", _connect=lambda **kw: calls.append(kw) or "conn")
    assert conn == "conn"
    assert calls[0]["options"] == INTERNALS_OPTION
    assert calls[0]["autocommit"] is True and calls[0]["host"] == "10.77.1.11"


def test_servers_that_reject_the_option_get_a_plain_retry():
    calls = []

    def fake(**kw):
        calls.append(kw)
        if "options" in kw:
            raise psycopg.OperationalError('unrecognized configuration parameter "allow_unsafe_internals"')
        return "plain"

    assert connect("h", _connect=fake) == "plain"
    assert "options" not in calls[1]


def test_other_connection_errors_propagate():
    def fake(**kw):
        raise psycopg.OperationalError("connection refused")

    with pytest.raises(psycopg.OperationalError, match="refused"):
        connect("h", _connect=fake)


def test_statement_timeout_is_added_when_requested():
    calls = []
    connect("h", statement_timeout="3s", _connect=lambda **kw: calls.append(kw) or "c")
    assert calls[0]["options"] == INTERNALS_OPTION + " -c statement_timeout=3s"
