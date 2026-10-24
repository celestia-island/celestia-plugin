# celestia-plugin

Python plugin/engine SDK for the celestia panel fabric.

The Python twin of `plana` / `plana-rpc-client`: any Python tool (scientific
computing, data processing, hardware wrappers) gains, with a small dependency,
the ability to run as a first-class celestia compute engine — dialable by the
panel over the established upstream-platform JSON-RPC/HTTP dialect, deployable
to evernight-managed nodes as a container workload.

Modules (v0.2.0):

| module | purpose |
|---|---|
| `celestia_plugin.jsonrpc` | JSON-RPC 2.0 types, error codes, dialect constants |
| `celestia_plugin.rpcserver` | threading HTTP server speaking the `POST /api/rpc?token=` dialect (single + batch, notification fan-out, `events.poll` with evicted-tail `dropped` count) and the `/health` endpoint (2xx gate for evernight `EngineBlueprint.health_path`) |
| `celestia_plugin.jobs` | job table: submit / status / cancel / result, progress callbacks, cancellation tokens, live-job ceiling (`max_queued`, `QueueFullError`) |
| `celestia_plugin.artifacts` | artifact registration + serving (list / fetch) |
| `celestia_plugin.config` | env/file configuration loading (bootstrap-root stays in 0600 env) |
| `celestia_plugin.engine` | `EngineServer` facade + fail-closed env prerequisites (`python -m` entry helper) |

Status: **pre-release** — v0.2.0 is consumed via git dependency / local path
only. PyPI publication is a separate, user-gated decision.

Upstream panel dialect reference: celestia-integration `world_service`
(`POST {base_url}/api/rpc?token={connection_token}`, batch responses fan out
notifications to subscribed browsers).
