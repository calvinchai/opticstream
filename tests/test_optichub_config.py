import json

from optichub.config import HubSettings


def test_load_creates_default_json(tmp_path, monkeypatch):
    path = tmp_path / "optichub.json"
    monkeypatch.delenv("REDIS_URL", raising=False)
    s = HubSettings.load(path)
    assert path.is_file()
    assert s.redis_url == "redis://127.0.0.1:6379/0"
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["redis_url"] == "redis://127.0.0.1:6379/0"
    assert data["prefect_server_url"] == "http://127.0.0.1/"


def test_redis_url_env_overrides_file(tmp_path, monkeypatch):
    path = tmp_path / "optichub.json"
    path.write_text(
        json.dumps({"redis_url": "redis://127.0.0.1:6379/0", "grpc_ping_timeout_ms": 500}),
        encoding="utf-8",
    )
    monkeypatch.setenv("REDIS_URL", "redis://redis.example:6379/1")
    s = HubSettings.load(path)
    assert s.redis_url == "redis://redis.example:6379/1"
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk["redis_url"] == "redis://127.0.0.1:6379/0"


def test_load_disk_ignores_redis_url_env(tmp_path, monkeypatch):
    path = tmp_path / "optichub.json"
    path.write_text(
        json.dumps({"redis_url": "redis://disk:6379/0", "grpc_ping_timeout_ms": 500}),
        encoding="utf-8",
    )
    monkeypatch.setenv("REDIS_URL", "redis://env:6379/1")
    disk = HubSettings.load_disk(path)
    assert disk.redis_url == "redis://disk:6379/0"


def test_empty_redis_url_env_does_not_override(tmp_path, monkeypatch):
    path = tmp_path / "optichub.json"
    path.write_text(
        json.dumps({"redis_url": "redis://file-host:6379/0", "grpc_ping_timeout_ms": 500}),
        encoding="utf-8",
    )
    monkeypatch.setenv("REDIS_URL", "   ")
    s = HubSettings.load(path)
    assert s.redis_url == "redis://file-host:6379/0"
