"""Env-worker **relay** — viva-api as the rendezvous between a client and a worker.

Plan §C/§C1 of vivarium-workbench's `docs/run-orchestration-consolidation.md`.

Today a hosted env worker dials back to the *workbench* pod, which works only
in-cluster: a laptop reaches this cluster through an SSM tunnel that is
laptop-initiated with no inbound path, so nothing in the cluster can dial it.
Reversing the direction does not help either — viva-api's ServiceAccount may
create Jobs but not Services, so a worker pod has no stable name to dial, which
is why dial-back exists at all.

So viva-api becomes the meeting point. It is in-cluster (the worker *can* reach
it) and reachable from a laptop over the tunnel that already exists. It binds
the listener, starts the Job pointing at itself, holds the socket, and exposes
one HTTP endpoint that forwards a call down it.

**Why this is far cheaper than the plan first priced it.** §C budgeted a
WebSocket, ALB upgrade handling, and a duplex bridge. None is needed: the
worker protocol is JSON-RPC over length-prefixed frames and is already **serial
by construction** — the client holds a mutex so "the next frame read is
unambiguously this call's reply". Request/response over plain HTTP is a faithful
carrier for a protocol that was already request/response.

**The serialization point is not a new invention.** It is that same mutex, kept
next to the same socket, one per worker. A viva-api restart drops the held
sockets — the same failure the workbench has today, not a regression.

The wire format here MUST match `vivarium_workbench/lib/env_worker_dialback.py`
and `env_worker_client.EnvWorker`; both ends of a protocol cannot drift
independently, and `tests/compose/test_env_worker_relay.py` pins the framing
against the literal bytes the workbench sends.
"""

from __future__ import annotations

import contextlib
import json
import secrets
import socket
import struct
import threading
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "HANDSHAKE_FRAME_CAP",
    "DialBackError",
    "DialBackListener",
    "RelayError",
    "RelayRegistry",
    "WorkerCallError",
    "WorkerConnection",
    "WorkerUnavailable",
    "registry",
]

#: The handshake frame is a fixed shape; anything larger is a bad actor or a
#: protocol mismatch, and is rejected before allocation.
HANDSHAKE_FRAME_CAP = 4096

#: Frames after the handshake carry a JSON-RPC message. Generous enough for a
#: real result, bounded so a confused peer cannot ask us to allocate a gigabyte.
MAX_FRAME = 64 * 1024 * 1024


class RelayError(Exception):
    """Base for relay faults."""


class DialBackError(RelayError):
    """No worker connected, or the one that connected failed the handshake."""


class WorkerUnavailable(RelayError):
    """The worker is gone: never registered, or closed its connection."""


class WorkerCallError(RelayError):
    """The worker answered with a JSON-RPC error. Carries its code and data."""

    def __init__(self, message: str, *, code: Any = None, data: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.data = data


class DialBackListener:
    """Listen for exactly one worker to dial back, and prove its token.

    A port of the workbench's listener of the same name. Ported rather than
    imported because viva-api does not depend on the workbench package — but it
    is the *same protocol*, and must stay so.
    """

    # S104 (bind-all-interfaces) is deliberate and is the point of the class: in
    # cluster the worker reaches us by pod IP, so binding only loopback would
    # make it unreachable. The port is ephemeral, accepts exactly one
    # connection, and closes to further ones the moment that connection proves
    # its token -- see accept(). Tests pass bind_host="127.0.0.1".
    def __init__(self, *, bind_host: str = "0.0.0.0", token: str | None = None) -> None:  # noqa: S104
        # token_hex, NOT token_urlsafe: the base64url alphabet contains "-", and
        # a token starting with one is parsed as an option flag by any
        # argv-based consumer -- an intermittent spawn failure that looks like a
        # cluster problem. Hex is alphanumeric, so the class is gone.
        self.token = token or secrets.token_hex(32)
        self._sock: socket.socket | None = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((bind_host, 0))  # port 0 -> the kernel picks a free one
        self._sock.listen(1)  # one worker, one connection
        self.port: int = self._sock.getsockname()[1]

    def accept(self, timeout: float = 300.0) -> socket.socket:
        """Block until a worker connects and proves the token; return its socket.

        ``timeout`` covers pod scheduling and image pull, so it is generous and
        deliberately separate from the per-call timeout.
        """
        if self._sock is None:
            raise DialBackError("listener is closed")
        self._sock.settimeout(timeout)
        try:
            conn, _peer = self._sock.accept()
        except TimeoutError:
            raise DialBackError(f"no worker connected within {timeout}s") from None
        except OSError as e:
            raise DialBackError(f"accept failed: {e}") from e
        try:
            self._verify(conn, timeout)
        except Exception:
            conn.close()
            raise
        # One worker per listener: stop listening so the port cannot be reused
        # by a second connection while this worker is live.
        self.close_listener()
        return conn

    def _verify(self, conn: socket.socket, timeout: float) -> None:
        conn.settimeout(timeout)
        hdr = _recv_exact(conn, 4)
        if hdr is None:
            raise DialBackError("worker closed before the handshake")
        (n,) = struct.unpack(">I", hdr)
        if n > HANDSHAKE_FRAME_CAP:
            raise DialBackError(f"handshake frame too large: {n} bytes")
        body = _recv_exact(conn, n)
        if body is None:
            raise DialBackError("worker closed mid-handshake")
        try:
            msg = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise DialBackError(f"malformed handshake: {e}") from e
        offered = msg.get("token") if isinstance(msg, dict) else None
        # Constant-time: a timing oracle on a token is a real, if unglamorous,
        # way to lose one.
        if not isinstance(offered, str) or not secrets.compare_digest(offered, self.token):
            raise DialBackError("handshake token mismatch")

    def close_listener(self) -> None:
        """Stop accepting. An already-accepted connection is unaffected."""
        if self._sock is not None:
            with contextlib.suppress(OSError):
                self._sock.close()
            self._sock = None

    def __enter__(self) -> DialBackListener:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close_listener()


@dataclass
class WorkerConnection:
    """One held worker socket, plus the lock that keeps its FIFO contract.

    The lock is the whole point. The worker reads one request and writes one
    reply; two concurrent callers would interleave frames and each would read
    the other's answer. That mutex exists in the workbench client today and is
    reproduced here rather than replaced, because it is the contract, not an
    implementation detail.
    """

    job_name: str
    sock: socket.socket
    lock: threading.Lock = field(default_factory=threading.Lock)
    _id: int = 0

    def call(self, method: str, params: dict[str, Any] | None = None, *, timeout: float = 300.0) -> Any:
        """Send one JSON-RPC request; return its ``result`` or raise."""
        with self.lock:
            self._id += 1
            rid = self._id
            self.sock.settimeout(timeout)
            self._send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params or {}})
            resp = self._recv()
            if resp is None:
                raise WorkerUnavailable("worker closed the connection")
            if resp.get("id") != rid:
                # Never paper over a desync by reading again: the socket is now
                # of unknown alignment and every later reply would be suspect.
                raise WorkerUnavailable(f"protocol desync: got id {resp.get('id')}, wanted {rid}")
            if "error" in resp:
                e = resp["error"] or {}
                raise WorkerCallError(e.get("message", "worker error"), code=e.get("code"), data=e.get("data"))
            return resp.get("result")

    def _send(self, obj: dict[str, Any]) -> None:
        body = json.dumps(obj).encode("utf-8")
        try:
            self.sock.sendall(struct.pack(">I", len(body)) + body)
        except OSError as e:
            raise WorkerUnavailable(f"send failed: {e}") from e

    def _recv(self) -> dict[str, Any] | None:
        hdr = _recv_exact(self.sock, 4)
        if hdr is None:
            return None
        (n,) = struct.unpack(">I", hdr)
        if n > MAX_FRAME:
            raise WorkerUnavailable(f"frame too large: {n} bytes")
        body = _recv_exact(self.sock, n)
        if body is None:
            return None
        try:
            decoded: dict[str, Any] = json.loads(body.decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as e:
            raise WorkerUnavailable(f"malformed frame: {e}") from e
        return decoded

    def close(self) -> None:
        """Best-effort shutdown. Never raises: this runs on teardown paths."""
        with contextlib.suppress(OSError):
            self.sock.close()


class RelayRegistry:
    """Process-local map of ``job_name`` -> held :class:`WorkerConnection`.

    In-memory on purpose, and the docstring says so rather than leaving it to be
    discovered: the *socket* cannot outlive the process holding it, so no
    amount of persistence would make these survive a restart. A viva-api roll
    drops every relayed worker, exactly as a workbench roll does today. Callers
    get :class:`WorkerUnavailable` and start a new one.
    """

    def __init__(self) -> None:
        self._conns: dict[str, WorkerConnection] = {}
        self._lock = threading.Lock()

    def register(self, conn: WorkerConnection) -> None:
        with self._lock:
            existing = self._conns.get(conn.job_name)
            self._conns[conn.job_name] = conn
        if existing is not None and existing is not conn:
            # A re-registration under the same name means the old socket is
            # orphaned; close it rather than leaking the fd until GC.
            existing.close()

    def get(self, job_name: str) -> WorkerConnection:
        with self._lock:
            conn = self._conns.get(job_name)
        if conn is None:
            raise WorkerUnavailable(f"no relayed worker registered as {job_name!r}")
        return conn

    def drop(self, job_name: str) -> bool:
        with self._lock:
            conn = self._conns.pop(job_name, None)
        if conn is None:
            return False
        conn.close()
        return True

    def names(self) -> list[str]:
        with self._lock:
            return sorted(self._conns)

    def close_all(self) -> None:
        with self._lock:
            conns = list(self._conns.values())
            self._conns.clear()
        for c in conns:
            c.close()


#: Process-wide registry. One per viva-api process, like the workbench's pool.
registry = RelayRegistry()


def _recv_exact(sock: socket.socket, n: int) -> bytes | None:
    buf = bytearray()
    while len(buf) < n:
        try:
            chunk = sock.recv(n - len(buf))
        except TimeoutError:
            raise WorkerUnavailable("timed out waiting for the worker") from None
        except OSError:
            return None
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)
