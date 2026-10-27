"""Connections to CockroachDB with internal tables enabled on v26+."""
from __future__ import annotations

from typing import Callable

import psycopg

INTERNALS_OPTION = "-c allow_unsafe_internals=on"


def connect(host: str, port: int = 26257, *, dbname: str = "defaultdb", timeout: int = 5,
            statement_timeout: str | None = None, _connect: Callable[..., object] | None = None):
    """Autocommit connection as root. v26 gates crdb_internal and system behind
    allow_unsafe_internals; v26 ignores unknown startup options, but retry without
    it if a server ever rejects the variable by name."""
    do_connect = _connect or psycopg.connect
    kwargs = dict(host=host, port=port, user="root", dbname=dbname, connect_timeout=timeout,
                  autocommit=True, sslmode="disable")
    options = INTERNALS_OPTION + (f" -c statement_timeout={statement_timeout}" if statement_timeout else "")
    try:
        return do_connect(**kwargs, options=options)
    except psycopg.Error as e:
        if "allow_unsafe_internals" not in str(e):
            raise
        return do_connect(**kwargs)
