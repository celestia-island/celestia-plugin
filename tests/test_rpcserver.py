"""RpcServer over real HTTP — token gate, dialect shapes, events, artifacts."""

import json
import time
import urllib.error
import urllib.request

import pytest

from celestia_plugin.artifacts import ArtifactStore
from celestia_plugin.jobs import JobTable
from celestia_plugin.rpcserver import RpcServer

TOKEN = "test-token-0123456789"


@pytest.fixture()
def server(tmp_path):
    """A fully wired server on an ephemeral port; yields (server, base_url)."""
    store = ArtifactStore(tmp_path / "artifacts")
    rpc = RpcServer(token=TOKEN, name="test-engine", version="0.1.0", artifacts=store)
    httpd = rpc.build_http_server("127.0.0.1", 0)
    host, port = httpd.server_address[:2]
    base = f"http://{host}:{port}"

    import threading

    thread = threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
    thread.start()
    yield rpc, base
    httpd.shutdown()
    httpd.server_close()


def rpc_call(base, method, params=None, token=TOKEN, expect_status=200):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}).encode()
    req = urllib.request.Request(
        f"{base}/api/rpc?token={token}", data=body, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def test_health_gate_answers_2xx(server):
    _, base = server
    with urllib.request.urlopen(f"{base}/health", timeout=5) as resp:
        payload = json.loads(resp.read().decode())
    assert resp.status == 200
    assert payload["ok"] is True
    assert payload["name"] == "test-engine"


def test_token_is_required_and_constant_time_checked(server):
    _, base = server
    status, payload = rpc_call(base, "engine.info", token="wrong-token")
    assert status == 401
    status, payload = rpc_call(base, "engine.info", token="")
    assert status == 401


def test_engine_info_builtin_lists_methods(server):
    _, base = server
    status, payload = rpc_call(base, "engine.info")
    assert status == 200
    assert payload["result"]["name"] == "test-engine"
    assert "events.poll" in payload["result"]["methods"]


def test_method_not_found(server):
    _, base = server
    status, payload = rpc_call(base, "no.such.method")
    assert status == 200
    assert payload["error"]["code"] == -32601


def test_handler_result_and_app_error(server):
    rpc, base = server

    @rpc.method("math.add")
    def add(params, _ctx):
        return {"sum": params["a"] + params["b"]}

    @rpc.method("math.guard")
    def guard(_params, _ctx):
        raise ValueError("bad input")

    status, payload = rpc_call(base, "math.add", {"a": 40, "b": 2})
    assert payload["result"] == {"sum": 42}

    status, payload = rpc_call(base, "math.guard")
    assert payload["error"]["code"] == -32603  # internal — unexpected exception
    assert "ValueError: bad input" in payload["error"]["message"]


def test_batch_answers_array(server):
    rpc, base = server
    rpc.register("ping.pong", lambda _p, _c: {"pong": True})
    body = json.dumps(
        [
            {"jsonrpc": "2.0", "id": 1, "method": "ping.pong"},
            {"jsonrpc": "2.0", "id": 2, "method": "ping.pong"},
        ]
    ).encode()
    req = urllib.request.Request(f"{base}/api/rpc?token={TOKEN}", data=body)
    with urllib.request.urlopen(req, timeout=5) as resp:
        payload = json.loads(resp.read().decode())
    assert isinstance(payload, list) and len(payload) == 2
    assert {row["id"] for row in payload} == {1, 2}


def test_notification_only_batch_answers_204(server):
    rpc, base = server
    seen = []
    rpc.register("note.take", lambda params, _c: seen.append(params) or None)
    body = json.dumps([{"jsonrpc": "2.0", "method": "note.take", "params": {"x": 1}}]).encode()
    req = urllib.request.Request(f"{base}/api/rpc?token={TOKEN}", data=body)
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 204
    assert seen == [{"x": 1}]


def test_events_ring_roundtrip(server):
    rpc, base = server
    rpc.emit("jobs.progress", {"job_id": "j1", "frac": 0.5})
    status, payload = rpc_call(base, "events.poll", {"since": 0})
    events = payload["result"]["events"]
    assert events[0]["method"] == "jobs.progress"
    nxt = payload["result"]["next"]
    status, payload = rpc_call(base, "events.poll", {"since": nxt})
    assert payload["result"]["events"] == []


def test_artifact_register_and_fetch(server):
    rpc, base = server
    store = rpc.artifacts
    job_dir = store.job_dir("job0001abcd")
    (job_dir / "report.json").write_text('{"ok": true}')
    artifact = store.register("job0001abcd", "report.json")

    # no token → 401 (the artifact id is addressing, not authorization)
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        urllib.request.urlopen(f"{base}/api/artifacts/{artifact.id}", timeout=5)
    assert excinfo.value.code == 401

    with urllib.request.urlopen(f"{base}/api/artifacts/{artifact.id}?token={TOKEN}", timeout=5) as resp:
        assert resp.status == 200
        assert json.loads(resp.read().decode()) == {"ok": True}


def test_unserializable_handler_result_answers_500_not_disconnect(server):
    """R2 N1: a handler returning a set used to TypeError past the JSON
    boundary and drop the connection. It must answer a structured 500 and
    keep serving."""
    rpc, base = server

    @rpc.method("bad.return")
    def bad(_params, _ctx):
        return {"trace": {"0x1f"}}  # a set is not JSON-serializable

    status, payload = rpc_call(base, "bad.return")
    assert status == 500
    assert payload["error"]["code"] == -32603
    assert "JSON-serializable" in payload["error"]["message"]
    # the server survives and keeps answering
    status, payload = rpc_call(base, "engine.info")
    assert status == 200


def test_batch_of_one_answers_array(server):
    """R2 N2: a batch with a single member is still a batch — the reply
    keeps the array shape (the chest fan-out lane rides it)."""
    rpc, base = server
    rpc.register("ping.pong", lambda _p, _c: {"pong": True})
    body = json.dumps([{"jsonrpc": "2.0", "id": 9, "method": "ping.pong"}]).encode()
    req = urllib.request.Request(f"{base}/api/rpc?token={TOKEN}", data=body)
    with urllib.request.urlopen(req, timeout=5) as resp:
        payload = json.loads(resp.read().decode())
    assert isinstance(payload, list) and payload[0]["result"] == {"pong": True}


def test_oversized_body_answers_413(server):
    """The size gate fires on the header, before any body byte is read —
    so the probe sends a huge Content-Length with only a stub of body and
    reads the early 413 off a raw socket (a full 8 MB send would just hit
    a broken pipe against a server that stopped reading)."""
    import socket

    _, base = server
    host, port = base.split("//")[1].split(":")
    sock = socket.create_connection((host, int(port)), timeout=10)
    try:
        head = (
            f"POST /api/rpc?token={TOKEN} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Content-Type: application/json\r\n"
            "Connection: close\r\n"
            f"Content-Length: {8 * 1024 * 1024 + 1}\r\n\r\n"
        ).encode()
        sock.sendall(head + b'{"jsonrpc":"2.0"')  # stub; the gate fires first
        data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
        assert data.startswith(b"HTTP/1.1 413"), data[:120]
    finally:
        sock.close()


def test_slow_client_socket_times_out(tmp_path):
    """R2 F5 red-test gap: with a short handler_timeout a connection that
    sends nothing loses the socket instead of pinning the thread."""
    import socket
    import time

    rpc = RpcServer(token=TOKEN, handler_timeout=0.5)
    httpd = rpc.build_http_server("127.0.0.1", 0)
    host, port = httpd.server_address[:2]
    import threading

    threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
    try:
        sock = socket.create_connection((host, port), timeout=5)
        sock.settimeout(5)
        start = time.time()
        assert sock.recv(1024) == b""  # server closed the silent connection
        assert time.time() - start < 3  # ~handler_timeout, not forever
        sock.close()
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_non_ascii_token_answers_401_not_broken_pipe(server):
    """hmac.compare_digest(str) raises on non-ASCII — the gate must answer
    401 instead of dropping the connection. Real clients percent-encode
    query strings, so that is the wire shape we exercise."""
    import urllib.parse

    _, base = server
    encoded = urllib.parse.quote("tökén-非ascii")
    req = urllib.request.Request(f"{base}/api/rpc?token={encoded}", data=b"{}")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            status = resp.status
    except urllib.error.HTTPError as exc:
        status = exc.code
    assert status == 401


def test_artifact_fetch_404(server):
    _, base = server
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        urllib.request.urlopen(f"{base}/api/artifacts/doesnotexist?token={TOKEN}", timeout=5)
    assert excinfo.value.code == 404


def test_wrong_path_404(server):
    _, base = server
    req = urllib.request.Request(f"{base}/api/other?token={TOKEN}", data=b"{}")
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        urllib.request.urlopen(req, timeout=5)
    assert excinfo.value.code == 404


def test_job_events_flow_into_ring():
    """The jobs table wired to emit() must land lifecycle events in the ring
    — that is the streaming lane chest pollers consume."""
    rpc = RpcServer(token=TOKEN, jobs=JobTable())
    job = rpc.jobs.submit("quick", lambda _job: {"ok": True})
    deadline = time.time() + 5
    while time.time() < deadline:
        rows = rpc.jobs.list()
        if rows[0]["state"] in ("succeeded", "failed", "cancelled"):
            break
        time.sleep(0.01)
    events = [payload for _seq, payload in rpc._events]
    kinds = [event["method"] for event in events]
    assert "jobs.succeeded" in kinds
    assert events[kinds.index("jobs.succeeded")]["params"]["job_id"] == job.id
