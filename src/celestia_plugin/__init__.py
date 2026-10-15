"""celestia-plugin — Python plugin/engine SDK for the celestia panel fabric."""

from .jsonrpc import (
    JSONRPC_VERSION,
    RpcError,
    RpcRequest,
    RpcResponse,
    error_response,
    result_response,
)
from .rpcserver import RpcServer, RequestContext
from .jobs import Job, JobTable, QueueFullError
from .artifacts import ArtifactStore
from .config import load_config
from .engine import EngineServer, artifacts_dir_from_env, ensure_prerequisites, token_from_env

__version__ = "0.1.0"

__all__ = [
    "JSONRPC_VERSION",
    "ArtifactStore",
    "EngineServer",
    "Job",
    "JobTable",
    "QueueFullError",
    "RequestContext",
    "RpcError",
    "RpcRequest",
    "RpcResponse",
    "RpcServer",
    "artifacts_dir_from_env",
    "ensure_prerequisites",
    "error_response",
    "load_config",
    "result_response",
    "token_from_env",
    "__version__",
]
