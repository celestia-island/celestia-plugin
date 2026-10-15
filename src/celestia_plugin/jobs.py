"""Job table — long-running compute work with progress and cancellation.

Engines wrap their heavy work in `JobTable.submit`; the job thread reports
progress through the handle and honours cooperative cancellation. The table
is the engine's single source of truth for `*.status` / `*.cancel` RPCs.
"""

from __future__ import annotations

import itertools
import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable

log = logging.getLogger("celestia_plugin.jobs")

# A job row's `state` vocabulary — closed set, add with care.
QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
FAILED = "failed"
CANCELLED = "cancelled"
_TERMINAL = {SUCCEEDED, FAILED, CANCELLED}


class CancelledError(RuntimeError):
    """Raised inside a job that observed its cancellation signal."""


@dataclass
class Job:
    """One submitted job. Engines receive this as their work handle."""

    id: str
    name: str
    state: str = QUEUED
    submitted_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    progress: float | None = None
    progress_message: str = ""
    result: Any = None
    error: str | None = None
    # Owner-only bits (never serialized by `snapshot`).
    cancel_event: threading.Event = field(default_factory=threading.Event, repr=False)
    _seq: "itertools.count" = field(default_factory=itertools.count, repr=False)

    def report_progress(self, frac: float | None, message: str = "") -> None:
        """Publish a progress step. `frac` is 0..1 or None (indeterminate)."""
        if frac is not None and not 0.0 <= frac <= 1.0:
            raise ValueError(f"progress frac out of range: {frac}")
        self.progress = frac
        self.progress_message = message

    def cancelled(self) -> bool:
        return self.cancel_event.is_set()

    def check_cancelled(self) -> None:
        """Raise `CancelledError` when a cancellation was requested."""
        if self.cancelled():
            raise CancelledError("job cancelled")

    def snapshot(self) -> dict[str, Any]:
        """Wire-safe view (no thread primitives)."""
        return {
            "job_id": self.id,
            "name": self.name,
            "state": self.state,
            "submitted_at": self.submitted_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "progress": self.progress,
            "progress_message": self.progress_message,
            "result": self.result if self.state == SUCCEEDED else None,
            "error": self.error,
        }


class JobTable:
    """Thread-backed job table. `max_workers` bounds concurrent RUNNING
    jobs with a semaphore (queued beyond it); `None` = unbounded."""

    def __init__(self, max_workers: int | None = None) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, Job] = {}
        self._order: list[str] = []
        self._slots = threading.Semaphore(max_workers) if max_workers else None
        self._on_event: Callable[[str, dict[str, Any]], None] | None = None

    def set_event_sink(self, sink: Callable[[str, dict[str, Any]], None] | None) -> None:
        """Receive lifecycle events as (kind, payload) — the RPC layer wires
        this to its notification ring so pollers can stream progress."""
        self._on_event = sink

    def submit(self, name: str, fn: Callable[[Job], Any]) -> Job:
        """Spawn `fn(handle)` on a daemon thread; returns the handle."""
        job = Job(id=uuid.uuid4().hex[:12], name=name)
        with self._lock:
            self._jobs[job.id] = job
            self._order.append(job.id)
        thread = threading.Thread(
            target=self._run, args=(job, fn), name=f"celestia-job-{job.id}", daemon=True
        )
        job.state = QUEUED
        self._emit("jobs.submitted", {"job_id": job.id, "name": name})
        thread.start()
        return job

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            return self._jobs.get(job_id)

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._jobs[jid].snapshot() for jid in self._order]

    def cancel(self, job_id: str) -> dict[str, Any]:
        """Request cooperative cancellation. Returns the observed state so a
        caller can tell accepted from too-late."""
        job = self.get(job_id)
        if job is None:
            return {"accepted": False, "reason": "unknown job"}
        if job.state in _TERMINAL:
            return {"accepted": False, "reason": f"already {job.state}"}
        job.cancel_event.set()
        self._emit("jobs.cancel_requested", {"job_id": job_id})
        return {"accepted": True, "state": job.state}

    def _run(self, job: Job, fn: Callable[[Job], Any]) -> None:
        if self._slots is not None:
            self._slots.acquire()
        try:
            if job.cancelled():
                raise CancelledError("cancelled before start")
            job.state = RUNNING
            job.started_at = time.time()
            self._emit("jobs.started", {"job_id": job.id})
            job.result = fn(job)
            job.state = SUCCEEDED
            self._emit("jobs.succeeded", {"job_id": job.id})
        except CancelledError as exc:
            job.state = CANCELLED
            job.error = str(exc)
            self._emit("jobs.cancelled", {"job_id": job.id})
        except Exception as exc:  # noqa: BLE001 - the table is the boundary
            job.state = FAILED
            job.error = f"{type(exc).__name__}: {exc}"
            log.exception("job %s (%s) failed", job.id, job.name)
            self._emit("jobs.failed", {"job_id": job.id, "error": job.error})
        finally:
            job.finished_at = time.time()
            if self._slots is not None:
                self._slots.release()

    def _emit(self, kind: str, payload: dict[str, Any]) -> None:
        if self._on_event is not None:
            try:
                self._on_event(kind, payload)
            except Exception:  # noqa: BLE001 - a sink must never kill a job
                log.exception("event sink raised for %s", kind)
