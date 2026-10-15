"""EngineServer — the one-liner facade that turns a Python tool into a
dialable celestia compute engine.

    engine = EngineServer(name="electrode", version="0.1.0", token=..., artifacts_dir=...)

    @engine.method("generate.start")
    def start(params, ctx):
        job = ctx.jobs.submit("generate", work(params))
        return {"job_id": job.id}

    engine.serve(host="127.0.0.1", port=8080)   # blocks; SIGTERM/Ctrl-C exits
"""

from __future__ import annotations

import logging
import os
import signal
import sys
from pathlib import Path
from typing import Any, Callable

from .artifacts import ArtifactStore
from .config import ConfigError, load_config
from .jobs import JobTable
from .rpcserver import MethodFn, RpcServer

log = logging.getLogger("celestia_plugin.engine")

DEFAULT_ENV_PREFIX = "CELESTIA_ENGINE_"


class EngineServer:
    def __init__(
        self,
        *,
        name: str,
        version: str,
        token: str,
        artifacts_dir: str | os.PathLike[str] | None = None,
        max_workers: int | None = 2,
        config_file: str | None = None,
    ) -> None:
        self.name = name
        self.version = version
        self.jobs = JobTable(max_workers=max_workers)
        self.artifacts = ArtifactStore(artifacts_dir) if artifacts_dir else None
        self.rpc = RpcServer(
            token=token, name=name, version=version, jobs=self.jobs, artifacts=self.artifacts
        )
        self.config = load_config(file_path=config_file) if config_file else load_config()

    def method(self, name: str) -> Callable[[MethodFn], MethodFn]:
        """Decorator form of `register` (delegates to the RPC layer)."""
        return self.rpc.method(name)

    def register(self, name: str, fn: MethodFn) -> None:
        self.rpc.register(name, fn)

    def serve(self, host: str = "127.0.0.1", port: int = 0) -> None:
        """Bind, print the bound address (evernight reports the dynamic host
        port the same way), and block until SIGTERM/SIGINT."""
        httpd = self.rpc.build_http_server(host, port)
        bound_host, bound_port = httpd.server_address[:2]
        log.info(
            "engine %s %s listening on %s:%s (token from env)", self.name, self.version,
            bound_host, bound_port,
        )
        print(
            f"[celestia-plugin] engine {self.name} {self.version} "
            f"listening on {bound_host}:{bound_port}",
            flush=True,
        )

        def _stop(signum: int, _frame: Any) -> None:
            log.info("signal %s — shutting down", signum)
            raise KeyboardInterrupt()

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, _stop)
        try:
            httpd.serve_forever(poll_interval=0.2)
        except KeyboardInterrupt:
            pass
        finally:
            httpd.server_close()
            log.info("engine stopped")


def token_from_env(env_var: str = "CELESTIA_ENGINE_TOKEN") -> str:
    """Read the engine token; refuse defaults (fail closed)."""
    token = os.environ.get(env_var, "").strip()
    if not token:
        raise ConfigError(
            f"env {env_var} is empty or unset — generate a per-engine secret "
            f"(e.g. `python -c 'import secrets; print(secrets.token_urlsafe(32))'`)"
        )
    return token


def artifacts_dir_from_env(env_var: str = "CELESTIA_ENGINE_ARTIFACTS") -> Path:
    raw = os.environ.get(env_var, "").strip()
    if not raw:
        raise ConfigError(f"env {env_var} is empty or unset — point it at a writable directory")
    path = Path(raw)
    if not path.is_dir():
        raise ConfigError(f"artifact directory does not exist: {path}")
    return path


def ensure_prerequisites(
    token_env: str = "CELESTIA_ENGINE_TOKEN",
    artifacts_env: str = "CELESTIA_ENGINE_ARTIFACTS",
) -> None:
    """Early, explicit failure for the two required env pieces — a bare
    `python -m service` on a misconfigured host must die before binding."""
    token_from_env(token_env)
    artifacts_dir_from_env(artifacts_env)


def main_entry(argv: list[str] | None = None, *, name: str, version: str,
               build: Callable[[str, Path], EngineServer]) -> int:
    """Shared `python -m <engine>` entry: parse args, build, serve.

    `build(token, artifacts_dir)` returns the configured EngineServer."""
    import argparse

    parser = argparse.ArgumentParser(prog=name)
    parser.add_argument("--host", default="127.0.0.1", help="bind address (evernight publishes this loopback)")
    parser.add_argument("--port", type=int, default=0, help="port (0 = runtime-assigned)")
    parser.add_argument("--token-env", default=DEFAULT_ENV_PREFIX + "TOKEN")
    parser.add_argument("--artifacts-env", default=DEFAULT_ENV_PREFIX + "ARTIFACTS")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    try:
        engine = build(token_from_env(args.token_env), artifacts_dir_from_env(args.artifacts_env))
    except ConfigError as exc:
        print(f"[celestia-plugin] configuration error: {exc}", file=sys.stderr)
        return 2
    engine.serve(host=args.host, port=args.port)
    return 0
