"""JSON-RPC 2.0 wire types and error codes.

The dialect is the one chest already speaks toward upstream platforms
(`POST {base}/api/rpc?token=…`; a response is a single JSON-RPC object or a
batch array whose extra elements are notifications chest fans out to
subscribed browsers). This module only fixes vocabulary — transport lives in
`rpcserver`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

JSONRPC_VERSION = "2.0"

# Standard JSON-RPC 2.0 error codes.
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# Server-application errors live in -32000..-32099.
APP_ERROR_BASE = -32000

# Valid codes for the `error` member (JSON-RPC 2.0 §5.1).
_ERROR_CODES = {
    PARSE_ERROR,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    INVALID_PARAMS,
    INTERNAL_ERROR,
} | set(range(APP_ERROR_BASE, APP_ERROR_BASE - 1000, -1))


class RpcError(Exception):
    """A controlled JSON-RPC error a method handler raises."""

    def __init__(self, code: int, message: str, data: Any = None) -> None:
        if code not in _ERROR_CODES:
            raise ValueError(f"error code {code} outside the JSON-RPC error ranges")
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data

    def to_payload(self) -> dict[str, Any]:
        err: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.data is not None:
            err["data"] = self.data
        return err


@dataclass(frozen=True)
class RpcRequest:
    method: str
    params: dict[str, Any] = field(default_factory=dict)
    # `None` means notification (JSON-RPC 2.0 §4.1: id MAY be null on the
    # wire; a absent id is a notification — both are folded to None here).
    id: Any = None
    is_notification: bool = False


@dataclass(frozen=True)
class RpcResponse:
    id: Any
    result: Any = None
    error: dict[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"jsonrpc": JSONRPC_VERSION, "id": self.id}
        if self.error is not None:
            payload["error"] = self.error
        else:
            payload["result"] = self.result
        return payload


def result_response(id: Any, result: Any) -> RpcResponse:  # noqa: A002 - wire name
    return RpcResponse(id=id, result=result)


def error_response(id: Any, code: int, message: str, data: Any = None) -> RpcResponse:  # noqa: A002
    resp = RpcResponse(id=id)
    object.__setattr__(resp, "error", RpcError(code, message, data).to_payload())
    return resp


def parse_message(raw: bytes | str) -> list[RpcRequest]:
    """Parse one request object or a batch array into requests.

    Raises `RpcError(PARSE_ERROR)` for non-JSON text and
    `RpcError(INVALID_REQUEST)` for structurally invalid entries (a batch
    with an invalid entry is itself invalid — JSON-RPC 2.0 §6).
    """
    try:
        payload = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RpcError(PARSE_ERROR, f"parse error: {exc}") from exc

    if isinstance(payload, list):
        if not payload:
            raise RpcError(INVALID_REQUEST, "empty batch")
        return [_parse_one(entry, batch=True) for entry in payload]
    return [_parse_one(payload, batch=False)]


def _parse_one(entry: Any, batch: bool) -> RpcRequest:
    if not isinstance(entry, dict):
        raise RpcError(INVALID_REQUEST, "request is not an object")
    if entry.get("jsonrpc") != JSONRPC_VERSION:
        raise RpcError(INVALID_REQUEST, "missing or wrong jsonrpc version")
    method = entry.get("method")
    if not isinstance(method, str) or not method:
        raise RpcError(INVALID_REQUEST, "missing method")
    if "id" in entry:
        rid = entry["id"]
        if rid is not None and not isinstance(rid, (str, int)):
            raise RpcError(INVALID_REQUEST, "id must be string or number")
        return RpcRequest(
            method=method,
            params=entry.get("params") or {},
            id=rid,
            is_notification=False,
        )
    if batch and ("id" not in entry):
        # A member-less batch entry is still a valid notification.
        pass
    return RpcRequest(method=method, params=entry.get("params") or {}, id=None, is_notification=True)
