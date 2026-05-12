import pytest

from optichub.dashboard import project_setup


class _FakeConfig:
    registered = False
    saved: list[tuple[str, bool]] = []
    instances: list["_FakeConfig"] = []

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.archive_path = kwargs.get("archive_path")
        self.dandiset_path = kwargs.get("dandiset_path")
        _FakeConfig.instances.append(self)

    @classmethod
    def register_type_and_schema(cls):
        cls.registered = True

    def save(self, block_name: str, overwrite: bool = False):
        _FakeConfig.saved.append((block_name, overwrite))


class _FakeStateService:
    opened_projects: list[str] = []

    def __init__(self, backend):
        self.backend = backend

    def open_project_by_parts(self, project_name: str):
        class _ProjectContext:
            def __enter__(self):
                _FakeStateService.opened_projects.append(project_name)

            def __exit__(self, exc_type, exc, tb):
                return False

        return _ProjectContext()


@pytest.fixture(autouse=True)
def _reset_fake_config():
    _FakeConfig.registered = False
    _FakeConfig.saved = []
    _FakeConfig.instances = []
    _FakeStateService.opened_projects = []


def test_create_oct_project_saves_block_and_creates_base_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(project_setup, "PSOCTScanConfig", _FakeConfig)
    monkeypatch.setattr(project_setup, "OCTProjectStateService", _FakeStateService)

    result = project_setup.create_oct_project(
        "octproject",
        state_backend=object(),
        project_base_path=tmp_path / "oct-project",
        grid_size_x_normal=2,
        grid_size_x_tilted=3,
        grid_size_y=4,
    )

    assert _FakeConfig.registered
    assert _FakeConfig.saved == [("octproject-psoct-config", True)]
    assert result.block_name == "octproject-psoct-config"
    assert result.redis_project_initialized
    assert _FakeStateService.opened_projects == ["octproject"]
    assert result.created == [tmp_path / "oct-project"]
    assert (tmp_path / "oct-project").is_dir()

    config = _FakeConfig.instances[0]
    assert config.project_name == "octproject"
    assert config.acquisition == {
        "grid_size_x_normal": 2,
        "grid_size_x_tilted": 3,
        "grid_size_y": 4,
    }


def test_create_lsm_project_saves_block_and_ensures_directories(tmp_path, monkeypatch):
    monkeypatch.setattr(project_setup, "LSMScanConfig", _FakeConfig)
    monkeypatch.setattr(project_setup, "LSMProjectStateService", _FakeStateService)
    output_path = tmp_path / "existing-output"
    output_path.mkdir()
    info_file = tmp_path / "metadata" / "info.mat"

    result = project_setup.create_lsm_project(
        "lsmproject",
        state_backend=object(),
        project_base_path=tmp_path / "lsm-project",
        info_file=info_file,
        output_path=output_path,
    )

    assert _FakeConfig.registered
    assert _FakeConfig.saved == [("lsmproject-lsm-config", True)]
    assert result.block_name == "lsmproject-lsm-config"
    assert result.redis_project_initialized
    assert _FakeStateService.opened_projects == ["lsmproject"]
    assert result.created == [tmp_path / "lsm-project", tmp_path / "metadata"]
    assert result.verified == [output_path]
    assert (tmp_path / "metadata").is_dir()

    config = _FakeConfig.instances[0]
    assert config.project_name == "lsmproject"
    assert config.info_file == info_file
    assert config.output_path == output_path


def test_create_project_requires_name():
    with pytest.raises(ValueError, match="Project name is required"):
        project_setup.create_oct_project("  ")
