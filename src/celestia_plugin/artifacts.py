"""Artifact store — per-job output files with wire-safe descriptors.

The engine writes outputs into a per-job directory inside the store root and
registers the ones it wants to expose; consumers list and fetch them over
the engine's HTTP surface (`/api/artifacts/...`). Path containment is
enforced — an artifact id never escapes the root.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger("celestia_plugin.artifacts")

_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")


@dataclass(frozen=True)
class Artifact:
    id: str
    job_id: str
    filename: str
    size: int
    sha256: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "job_id": self.job_id,
            "filename": self.filename,
            "size": self.size,
            "sha256": self.sha256,
        }


class ArtifactError(Exception):
    pass


class ArtifactStore:
    def __init__(self, root: str | os.PathLike[str]) -> None:
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._artifacts: dict[str, Artifact] = {}
        self._by_job: dict[str, list[str]] = {}

    def job_dir(self, job_id: str) -> Path:
        """The directory an engine writes this job's outputs into."""
        if not _ID_RE.match(job_id):
            raise ArtifactError(f"invalid job id: {job_id!r}")
        path = self.root / job_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def register(self, job_id: str, filename: str) -> Artifact:
        """Expose one file already written under the job's directory."""
        if not _ID_RE.match(job_id):
            raise ArtifactError(f"invalid job id: {job_id!r}")
        if not filename or "/" in filename or "\\" in filename or filename.startswith("."):
            raise ArtifactError(f"invalid artifact filename: {filename!r}")
        path = self.root / job_id / filename
        if path.is_symlink():
            # A symlink swapped into the job dir would make us hash (and the
            # HTTP face serve) files outside the store root.
            raise ArtifactError(f"artifact must be a regular file: {filename}")
        if not path.is_file():
            raise ArtifactError(f"artifact file missing: {filename}")
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        artifact = Artifact(
            id=uuid.uuid4().hex[:12],
            job_id=job_id,
            filename=filename,
            size=path.stat().st_size,
            sha256=digest,
        )
        self._artifacts[artifact.id] = artifact
        self._by_job.setdefault(job_id, []).append(artifact.id)
        return artifact

    def get(self, artifact_id: str) -> Artifact | None:
        return self._artifacts.get(artifact_id)

    def path_of(self, artifact: Artifact) -> Path:
        path = (self.root / artifact.job_id / artifact.filename).resolve()
        if not path.is_relative_to(self.root):  # containment, belt and braces
            raise ArtifactError("artifact path escapes the store root")
        return path

    def list(self, job_id: str | None = None) -> list[dict[str, Any]]:
        if job_id is None:
            ids = [a.id for a in self._artifacts.values()]
        else:
            ids = self._by_job.get(job_id, [])
        return [self._artifacts[a].to_payload() for a in ids]
