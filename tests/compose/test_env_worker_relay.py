"""Env-worker relay (plan §C/§C1): viva-api as the rendezvous.

The framing here MUST match the workbench's
``vivarium_workbench/lib/env_worker_dialback.py`` and ``env_worker_client``.
Both ends of a protocol cannot drift independently, so these tests speak the
wire format literally — 4-byte big-endian length prefix, JSON body — rather than
using a helper that could drift alongside the implementation.
"""

from __future__ import annotations

import json
import socket
import struct
import threading
from collections.abc import Iterator
from typing import Any

import pytest

from viva_api.compose import env_worker_relay as relay


def _frame(obj: dict[str, Any]) -> bytes:
    body = json.dumps(obj).encode("utf-8")
    return struct.pack(">I", len(body)) + body


def _read_frame(sock: socket.socket) -> dict[str, Any]:
    hdr = b""
    while len(hdr) < 4:
        hdr += sock.recv(4 - len(hdr))
    (n,) = struct.unpack(">I", hdr)
    body = b""
    while len(body) < n:
        body += sock.recv(n - len(body))
    decoded: dict[str, Any] = json.loads(body.decode("utf-8"))
    return decoded


class _FakeWorker(threading.Thread):
    """Dials back, handshakes, then answers requests like a real worker."""

    def __init__(
        self,
        port: int,
        token: str,
        *,
        replies: dict[str, Any] | None = None,
        bad_token: bool = False,
    ) -> None:
        super().__init__(daemon=True)
        self.port, self.token, self.bad_token = port, token, bad_token
        self.replies = replies or {}
        self.seen: list[dict[str, Any]] = []
        self.sock: socket.socket | None = None

    def run(self) -> None:
        s = socket.create_connection(("127.0.0.1", self.port), timeout=10)
        self.sock = s
        s.sendall(_frame({"token": "wrong" if self.bad_token else self.token}))
        if self.bad_token:
            return
        while True:
            try:
                req = _read_frame(s)
            except (OSError, struct.error, ValueError):
                return
            self.seen.append(req)
            reply = self.replies.get(str(req.get("method")))
            if reply is None:
                reply = {"jsonrpc": "2.0", "id": req.get("id"), "result": {"echo": req.get("method")}}
            elif callable(reply):
                reply = reply(req)
            s.sendall(_frame(reply))


@pytest.fixture
def listener() -> Iterator[relay.DialBackListener]:
    lst = relay.DialBackListener(bind_host="127.0.0.1")
    yield lst
    lst.close_listener()


def test_handshake_accepts_the_minted_token_and_returns_a_usable_socket(listener: relay.DialBackListener) -> None:
    w = _FakeWorker(listener.port, listener.token)
    w.start()
    sock = listener.accept(timeout=10)
    conn = relay.WorkerConnection(job_name="j", sock=sock)
    assert conn.call("list_generators") == {"echo": "list_generators"}
    assert w.seen[0]["method"] == "list_generators"
    assert w.seen[0]["jsonrpc"] == "2.0"
    conn.close()


def test_a_wrong_token_is_refused_before_the_protocol_is_reached(listener: relay.DialBackListener) -> None:
    """A listening port is an attack surface; the handshake is not optional."""
    w = _FakeWorker(listener.port, listener.token, bad_token=True)
    w.start()
    with pytest.raises(relay.DialBackError, match="token mismatch"):
        listener.accept(timeout=10)


def test_accept_times_out_when_nothing_dials_back(listener: relay.DialBackListener) -> None:
    with pytest.raises(relay.DialBackError, match="no worker connected"):
        listener.accept(timeout=0.25)


def test_token_is_hex_so_it_can_never_start_with_a_dash(listener: relay.DialBackListener) -> None:
    """token_urlsafe's alphabet contains '-', and a token starting with one is
    parsed as an option flag by any argv-based consumer — an intermittent spawn
    failure that looks like a cluster problem."""
    int(listener.token, 16)  # raises unless pure hex
    assert len(listener.token) == 64


def test_a_worker_error_is_surfaced_with_its_code_and_data(listener: relay.DialBackListener) -> None:
    w = _FakeWorker(
        listener.port,
        listener.token,
        replies={
            "boom": lambda req: {
                "jsonrpc": "2.0",
                "id": req["id"],
                "error": {"message": "no such generator", "code": -32001, "data": {"ref": "x"}},
            },
        },
    )
    w.start()
    conn = relay.WorkerConnection(job_name="j", sock=listener.accept(timeout=10))
    with pytest.raises(relay.WorkerCallError) as ei:
        conn.call("boom")
    assert ei.value.code == -32001
    assert ei.value.data == {"ref": "x"}
    conn.close()


def test_an_id_mismatch_is_a_desync_and_is_never_papered_over(listener: relay.DialBackListener) -> None:
    """Reading again after a desync would leave the socket of unknown alignment
    and make every later reply suspect."""
    w = _FakeWorker(
        listener.port,
        listener.token,
        replies={
            "skew": lambda req: {"jsonrpc": "2.0", "id": 999, "result": "wrong"},
        },
    )
    w.start()
    conn = relay.WorkerConnection(job_name="j", sock=listener.accept(timeout=10))
    with pytest.raises(relay.WorkerUnavailable, match="desync"):
        conn.call("skew")
    conn.close()


def test_a_closed_worker_reports_unavailable_not_a_hang(listener: relay.DialBackListener) -> None:
    w = _FakeWorker(listener.port, listener.token)
    w.start()
    conn = relay.WorkerConnection(job_name="j", sock=listener.accept(timeout=10))
    assert w.sock is not None
    w.sock.close()
    with pytest.raises(relay.WorkerUnavailable):
        conn.call("anything")
    conn.close()


def test_concurrent_calls_are_serialized_per_worker(listener: relay.DialBackListener) -> None:
    """The worker reads one request and writes one reply. Two concurrent callers
    would interleave frames and each read the other's answer — this lock IS the
    FIFO contract, not an implementation detail."""
    order: list[str] = []

    def _slow(req: dict[str, Any]) -> dict[str, Any]:
        order.append(f"start:{req['params']['tag']}")
        threading.Event().wait(0.05)
        order.append(f"end:{req['params']['tag']}")
        return {"jsonrpc": "2.0", "id": req["id"], "result": req["params"]["tag"]}

    w = _FakeWorker(listener.port, listener.token, replies={"slow": _slow})
    w.start()
    conn = relay.WorkerConnection(job_name="j", sock=listener.accept(timeout=10))
    results: list[str] = []
    threads = [
        threading.Thread(target=lambda t=t: results.append(conn.call("slow", {"tag": t}))) for t in ("a", "b", "c")
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert sorted(results) == ["a", "b", "c"]
    # Never interleaved: every start is immediately followed by its own end.
    for i in range(0, len(order), 2):
        assert order[i].split(":")[1] == order[i + 1].split(":")[1], order
    conn.close()


# --- registry -------------------------------------------------------------- #


def test_registry_get_of_an_unknown_worker_is_unavailable_not_none() -> None:
    r = relay.RelayRegistry()
    with pytest.raises(relay.WorkerUnavailable, match="no relayed worker"):
        r.get("nope")


def test_registry_drop_is_idempotent_and_reports_whether_it_had_one() -> None:
    a, b = socket.socketpair()
    r = relay.RelayRegistry()
    r.register(relay.WorkerConnection(job_name="j", sock=a))
    assert r.names() == ["j"]
    assert r.drop("j") is True
    assert r.drop("j") is False
    assert r.names() == []
    b.close()


def test_re_registering_a_name_closes_the_orphaned_socket() -> None:
    """Otherwise the old fd leaks until GC, and on a busy relay that is a real
    descriptor exhaustion path."""
    a1, b1 = socket.socketpair()
    a2, b2 = socket.socketpair()
    r = relay.RelayRegistry()
    r.register(relay.WorkerConnection(job_name="j", sock=a1))
    r.register(relay.WorkerConnection(job_name="j", sock=a2))
    with pytest.raises(OSError):
        a1.send(b"x")  # closed
    a2.send(b"x")  # still live
    r.close_all()
    b1.close()
    b2.close()
