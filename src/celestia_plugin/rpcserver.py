"""HTTP transport — the upstream-platform JSON-RPC dialect over stdlib only.

Dialect (proven by celestia-integration's world_service, which chest dials
in production):

- `POST /api/rpc?token=<connection_token>` — token checked constant-time;
  body is one JSON-RPC request or a batch array;
- a reply is a single JSON-RPC response object, or an array when the request
  was a batch (extra elements may be notifications — chest fans those out to
  subscribed browsers, which is how progress streams reach the UI);
- notifications execute without a reply slot; a batch that produced no
  replies answers `204 No Content`;
- `GET /health` answers 2xx with a payload (the evernight
  `EngineBlueprint.health_path` gate only asks for 2xx).

`RpcServer` is deliberately transport-thin: methods are plain callables
`fn(params, ctx) -> dict`, the job table and artifact store ride in `ctx`.
"""

from __future__ import annotations

import hmac
import json
import logging
import threading
import time
import urllib.parse
from collections import deque
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

from .jsonrpc import (
    INTERNAL_ERROR,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    PARSE_ERROR,
    JSONRPC_VERSION,
    RpcError,
    RpcRequest,
    parse_message,
    result_response,
)
from .artifacts import ArtifactError, ArtifactStore
from .jobs import JobTable

log = logging.getLogger("celestia_plugin.rpc")

MethodFn = Callable[[dict[str, Any], "RequestContext"], Any]

# Notification ring size — a poller that falls further behind than this
# loses the tail, by design (progress streams are lossy, states are not).
_EVENT_RING = 1024


@dataclass
class RequestContext:
    """What a method handler gets besides params."""

    jobs: JobTable | None
    artifacts: ArtifactStore | None
    server_name: str
    server_version: str
    remote_addr: str


class RpcServer:
    """The dialable engine surface. Owns the HTTP listener, the method
    table, the event ring, and (optionally) the job table + artifact store
    it exposes to handlers via `RequestContext`."""

    def __init__(
        self,
        *,
        token: str,
        name: str = "celestia-engine",
        version: str = "0.0.0",
        jobs: JobTable | None = None,
        artifacts: ArtifactStore | None = None,
        handler_timeout: float = 30.0,
    ) -> None:
        if not token:
            raise ValueError("refusing to serve without a token — generate a per-engine secret")
        self.token = token
        self.name = name
        self.version = version
        self.handler_timeout = handler_timeout
        self.jobs = jobs
        self.artifacts = artifacts
        self._methods: dict[str, MethodFn] = {}
        self._events: deque[tuple[int, dict[str, Any]]] = deque(maxlen=_EVENT_RING)
        self._event_seq = 0
        self._event_lock = threading.Lock()
        self._started_at = time.time()
        self.max_body_bytes = 8 * 1024 * 1024
        if jobs is not None:
            jobs.set_event_sink(self.emit)
        # Built-ins every engine carries.
        self.register("events.poll", self._events_poll)
        self.register("engine.info", self._engine_info)

    # ── registration ────────────────────────────────────────────────

    def register(self, method: str, fn: MethodFn) -> None:
        if not method or method.startswith("/"):
            raise ValueError(f"invalid method name: {method!r}")
        if method in self._methods:
            raise ValueError(f"method already registered: {method}")
        self._methods[method] = fn

    def method(self, method: str) -> Callable[[MethodFn], MethodFn]:
        """Decorator form of `register`."""

        def deco(fn: MethodFn) -> MethodFn:
            self.register(method, fn)
            return fn

        return deco

    def emit(self, method: str, params: dict[str, Any]) -> None:
        """Push one notification into the ring (progress, lifecycle…)."""
        with self._event_lock:
            self._event_seq += 1
            seq = self._event_seq
            self._events.append((seq, {"jsonrpc": JSONRPC_VERSION, "method": method, "params": params}))

    # ── built-ins ───────────────────────────────────────────────────

    def _events_poll(self, params: dict[str, Any], _ctx: RequestContext) -> dict[str, Any]:
        since = params.get("since", 0)
        if isinstance(since, bool) or not isinstance(since, int) or since < 0:
            raise RpcError(-32000, "since must be a non-negative integer")
        with self._event_lock:
            events = [payload for seq, payload in self._events if seq > since]
            nxt = self._event_seq
            # Lossy-by-design ring made visible: how many events in the
            # poller's gap were evicted before the oldest survivor. A
            # consumer that cares can detect the hole and resync state.
            # (Known limit: seq is per-process — after an engine restart a
            # poller holding a stale `since` sees no drops for the new
            # generation's early events. An epoch/instance id is the fix
            # if that ever matters in practice.)
            oldest = self._events[0][0] if self._events else nxt + 1
            dropped = max(0, oldest - since - 1)
        return {"events": events, "next": nxt, "dropped": dropped}

    def _engine_info(self, _params: dict[str, Any], _ctx: RequestContext) -> dict[str, Any]:
        return {"name": self.name, "version": self.version, "methods": sorted(self._methods)}

    # ── dispatch ────────────────────────────────────────────────────

    def _dispatch(self, req: RpcRequest, ctx: RequestContext) -> Any:
        """Returns an RpcResponse-shaped payload, or None for notifications."""
        fn = self._methods.get(req.method)
        if fn is None:
            if req.is_notification:
                return None
            from .jsonrpc import error_response

            return error_response(req.id, METHOD_NOT_FOUND, f"method not found: {req.method}").to_payload()
        try:
            result = fn(req.params if isinstance(req.params, dict) else {}, ctx)
        except RpcError as exc:
            if req.is_notification:
                return None
            from .jsonrpc import error_response

            return error_response(req.id, exc.code, exc.message, exc.data).to_payload()
        except Exception as exc:  # noqa: BLE001 - the server is the boundary
            log.exception("method %s raised", req.method)
            if req.is_notification:
                return None
            from .jsonrpc import error_response

            # Deliberate: engines are internal, post-auth surfaces, and the
            # operator's panel needs the real reason more than secrecy from
            # authenticated callers. A public-facing host would redact here.
            return error_response(
                req.id, INTERNAL_ERROR, f"{type(exc).__name__}: {exc}"
            ).to_payload()
        if req.is_notification:
            return None
        return result_response(req.id, result).to_payload()

    def handle_payload(self, raw: bytes, remote_addr: str) -> tuple[int, Any]:
        """Process one POST body. Returns (http_status, json_payload|None)."""
        ctx = RequestContext(
            jobs=self.jobs,
            artifacts=self.artifacts,
            server_name=self.name,
            server_version=self.version,
            remote_addr=remote_addr,
        )
        try:
            requests = parse_message(raw)
        except RpcError as exc:
            from .jsonrpc import error_response

            return 200, error_response(None, exc.code, exc.message).to_payload()
        # Per-call local, never instance state: ThreadingHTTPServer runs
        # handle_payload concurrently, and a shared flag would let a slow
        # single request inherit a concurrent batch's reply shape.
        was_batch = raw.lstrip().startswith(b"[")

        replies: list[dict[str, Any]] = []
        for req in requests:
            reply = self._dispatch(req, ctx)
            if reply is not None:
                replies.append(reply)
        if not replies:
            return 204, None
        # A bare request answers a single object; a batch (even of one)
        # answers an array — the chest fan-out lane rides the array shape.
        return 200, (replies if was_batch else replies[0])

    # ── HTTP plumbing ───────────────────────────────────────────────

    def build_http_server(self, host: str, port: int) -> ThreadingHTTPServer:
        rpc = self

        class _Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"
            # Slow-client (slowloris) defense: a connection that stalls this
            # long loses the socket instead of pinning a thread forever.
            timeout = rpc.handler_timeout

            def log_message(self, fmt: str, *args: Any) -> None:  # silence stderr
                log.debug("%s " + fmt, self.address_string(), *args)

            def _send_json(self, status: int, payload: Any) -> None:
                if status >= 400:
                    # An early answer leaves the request body unread; on a
                    # keep-alive connection that body would be parsed as the
                    # next request line and poison whatever rides the pooled
                    # connection next. Half-close instead of draining (the
                    # wire announcement rides the status line below,
                    # RFC 7230 §6.6 SHOULD).
                    self.close_connection = True
                try:
                    body = b"" if payload is None else json.dumps(payload).encode("utf-8")
                except (TypeError, ValueError, RecursionError):
                    # A handler returned something unserializable (or a
                    # circular/degenerate structure) — answer a structured
                    # 500 instead of dropping the connection.
                    status, body = 500, json.dumps(
                        {"jsonrpc": JSONRPC_VERSION, "id": None,
                         "error": {"code": INTERNAL_ERROR,
                                   "message": "handler result is not JSON-serializable"}}
                    ).encode("utf-8")
                self.send_response(status)
                if status >= 400:
                    self.send_header("Connection", "close")
                if body:
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                elif status != 204:
                    # 204 MUST NOT carry Content-Length (RFC 7230 §3.3.2);
                    # any other empty reply still needs explicit framing.
                    self.send_header("Content-Length", "0")
                self.end_headers()
                if body:
                    self.wfile.write(body)

            def _authorized(self) -> bool:
                query = urllib.parse.urlparse(self.path).query
                token = urllib.parse.parse_qs(query).get("token", [""])[0]
                if not token:
                    return False
                # bytes comparison: str compare_digest raises on non-ASCII,
                # which would drop the connection instead of answering 401
                return hmac.compare_digest(token.encode("utf-8"), rpc.token.encode("utf-8"))

            def do_POST(self) -> None:  # noqa: N802 - stdlib contract
                path = urllib.parse.urlparse(self.path).path
                if path != "/api/rpc":
                    self._send_json(404, {"error": "not found"})
                    return
                if not self._authorized():
                    self._send_json(401, {"error": "unauthorized"})
                    return
                try:
                    length = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    self._send_json(400, {"error": "Content-Length is not a number"})
                    return
                if length < 0:
                    self._send_json(400, {"error": "Content-Length is negative"})
                    return
                if length > rpc.max_body_bytes:
                    self._send_json(413, {"error": f"body exceeds {rpc.max_body_bytes} bytes"})
                    return
                body = self.rfile.read(length) if length else b""
                status, payload = rpc.handle_payload(body, self.client_address[0])
                self._send_json(status, payload)

            def do_GET(self) -> None:  # noqa: N802
                parsed = urllib.parse.urlparse(self.path)
                if parsed.path == "/health":
                    self._send_json(200, rpc.health_payload())
                    return
                if rpc.artifacts is not None and parsed.path.startswith("/api/artifacts/"):
                    # Same token gate as the RPC face — the artifact id is
                    # an addressing scheme, not the authorization.
                    if not self._authorized():
                        self._send_json(401, {"error": "unauthorized"})
                        return
                    artifact_id = parsed.path.rsplit("/", 1)[-1]
                    artifact = rpc.artifacts.get(artifact_id)
                    if artifact is None:
                        self._send_json(404, {"error": "unknown artifact"})
                        return
                    try:
                        path = rpc.artifacts.path_of(artifact)
                        data = path.read_bytes()
                    except (ArtifactError, OSError):
                        # containment trip (e.g. a symlink swapped in after
                        # registration) is a server fault, not a hang
                        self._send_json(500, {"error": "artifact unreadable"})
                        return
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return
                self._send_json(404, {"error": "not found"})

        server = ThreadingHTTPServer((host, port), _Handler)
        server.daemon_threads = True
        return server

    def health_payload(self) -> dict[str, Any]:
        jobs_summary: dict[str, Any] = {}
        if self.jobs is not None:
            rows = self.jobs.list()
            jobs_summary = {
                "total": len(rows),
                "running": sum(1 for r in rows if r["state"] == "running"),
            }
        return {
            "ok": True,
            "name": self.name,
            "version": self.version,
            "uptime_s": round(time.time() - self._started_at, 1),
            "jobs": jobs_summary,
        }
