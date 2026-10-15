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
from .jobs import Job, JobTable
from .artifacts import ArtifactStore
from .config import load_config

__version__ = "0.1.0"

__all__ = [
    "JSONRPC_VERSION",
    "ArtifactStore",
    "Job",
    "JobTable",
    "RequestContext",
    "RpcError",
    "RpcRequest",
    "RpcResponse",
    "RpcServer",
    "error_response",
    "load_config",
    "result_response",
    "__version__",
]
