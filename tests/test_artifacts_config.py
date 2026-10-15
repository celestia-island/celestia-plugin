"""Artifact store containment and integrity; config loading rules."""

import json
import os

import pytest

from celestia_plugin.artifacts import ArtifactError, ArtifactStore
from celestia_plugin.config import ConfigError, load_config, require


def test_register_computes_size_and_sha(tmp_path):
    store = ArtifactStore(tmp_path / "root")
    (store.job_dir("jobabc123") / "out.bin").write_bytes(b"\x00" * 128)
    artifact = store.register("jobabc123", "out.bin")
    assert artifact.size == 128
    assert len(artifact.sha256) == 64
    assert store.get(artifact.id).filename == "out.bin"


def test_register_missing_file_rejected(tmp_path):
    store = ArtifactStore(tmp_path / "root")
    store.job_dir("jobabc123")
    with pytest.raises(ArtifactError):
        store.register("jobabc123", "ghost.bin")


def test_filename_traversal_rejected(tmp_path):
    store = ArtifactStore(tmp_path / "root")
    (tmp_path / "secret.txt").write_text("top secret")
    with pytest.raises(ArtifactError):
        store.register("jobabc123", "../secret.txt")
    with pytest.raises(ArtifactError):
        store.register("jobabc123", "sub/dir.txt")


def test_invalid_job_id_rejected(tmp_path):
    store = ArtifactStore(tmp_path / "root")
    with pytest.raises(ArtifactError):
        store.job_dir("../escape")
    with pytest.raises(ArtifactError):
        store.register("BAD_ID", "x.bin")


def test_list_scopes_by_job(tmp_path):
    store = ArtifactStore(tmp_path / "root")
    for job in ("job000000001", "job000000002"):
        (store.job_dir(job) / "f.bin").write_bytes(b"x")
        store.register(job, "f.bin")
    assert len(store.list()) == 2
    assert len(store.list("job000000001")) == 1


def test_config_env_overlay_and_json_values(monkeypatch):
    monkeypatch.setenv("CELESTIA_PLUGIN_PORT", "8080")
    monkeypatch.setenv("CELESTIA_PLUGIN_TAGS", '["a","b"]')
    monkeypatch.setenv("CELESTIA_PLUGIN_NAME", "plain-string")
    monkeypatch.setenv("OTHER_PREFIX_X", "1")
    cfg = load_config(env_prefix="CELESTIA_PLUGIN_")
    assert cfg["port"] == 8080
    assert cfg["tags"] == ["a", "b"]
    assert cfg["name"] == "plain-string"
    assert "x" not in cfg


def test_config_env_wins_over_file(tmp_path, monkeypatch):
    cfg_file = tmp_path / "plugin.json"
    cfg_file.write_text(json.dumps({"port": 1, "from_file": True}))
    monkeypatch.setenv("CELESTIA_PLUGIN_PORT", "2")
    cfg = load_config(env_prefix="CELESTIA_PLUGIN_", file_path=str(cfg_file))
    assert cfg["port"] == 2
    assert cfg["from_file"] is True


def test_config_missing_file_and_required_key(tmp_path):
    with pytest.raises(ConfigError):
        load_config(file_path=str(tmp_path / "absent.json"))
    (tmp_path / "c.json").write_text("{}")
    cfg = load_config(file_path=str(tmp_path / "c.json"))
    with pytest.raises(ConfigError):
        require(cfg, "token", str)
