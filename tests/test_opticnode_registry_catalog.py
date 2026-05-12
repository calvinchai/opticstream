"""ModuleRegistry catalog helpers for GUI / ListModules-style views."""

from __future__ import annotations

import pytest

from opticnode.app.config import Settings
from opticnode.modules.base import ModuleConfig, ModuleRegistry, ModuleState, LoopModule


class _DummyConfig(ModuleConfig):
    foo: int = 1


class _DummyLoop(LoopModule):
    name = "dummy"
    Config = _DummyConfig

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(timeout=1.0)


def test_registered_module_names_sorted() -> None:
    settings = Settings()
    reg = ModuleRegistry(settings, gui_mode=False)
    reg.register_factory("zebra", _DummyLoop)
    reg.register_factory("alpha", _DummyLoop)
    assert reg.registered_module_names() == ["alpha", "zebra"]


def test_status_for_unknown_raises() -> None:
    settings = Settings()
    reg = ModuleRegistry(settings, gui_mode=False)
    reg.register_factory("dummy", _DummyLoop)
    with pytest.raises(KeyError):
        reg.status_for("nope")


def test_status_for_stopped_synthetic() -> None:
    settings = Settings()
    reg = ModuleRegistry(settings, gui_mode=False)
    reg.register_factory("dummy", _DummyLoop)
    st = reg.status_for("dummy")
    assert st.name == "dummy"
    assert st.state == ModuleState.STOPPED
    assert st.started_at is None
    assert st.error == ""
    assert st.config == {"foo": 1}


def test_status_for_running_matches_live() -> None:
    settings = Settings()
    reg = ModuleRegistry(settings, gui_mode=False)
    reg.register_factory("dummy", _DummyLoop)
    reg.start("dummy", {"foo": 42})
    st = reg.status_for("dummy")
    assert st.state == ModuleState.RUNNING
    assert st.config == {"foo": 42}
    assert st.started_at is not None
    reg.stop("dummy")
    st2 = reg.status_for("dummy")
    assert st2.state == ModuleState.STOPPED
