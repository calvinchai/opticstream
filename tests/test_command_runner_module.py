from __future__ import annotations

import sys
import time

import pytest

from opticnode.modules.command_runner import CommandRunnerModule


def _wait_for_terminal_result(module: CommandRunnerModule, job_id: str) -> dict:
    deadline = time.time() + 5.0
    while time.time() < deadline:
        result = module.get_job_result(job_id)
        if result and result["status"] in {"finished", "failed", "timed_out"}:
            return result
        time.sleep(0.01)
    raise AssertionError(f"job {job_id} did not finish")


def test_command_runner_stores_completed_result() -> None:
    module = CommandRunnerModule()
    module.start({"max_results": 10})

    job_id = module.submit_job(
        {
            "command": sys.executable,
            "args": ["-c", "print('hello')"],
            "timeout_s": 5.0,
        }
    )

    result = _wait_for_terminal_result(module, job_id)
    assert result["status"] == "finished"
    assert result["exit_code"] == 0
    assert result["stdout"] == "hello\n"
    assert module.list_job_results()[0]["job_id"] == job_id


def test_command_runner_times_out() -> None:
    module = CommandRunnerModule()
    module.start({"max_results": 10, "default_timeout_s": 1.0, "max_timeout_s": 5.0})

    job_id = module.submit_job(
        {
            "command": sys.executable,
            "args": ["-c", "import time; time.sleep(1)"],
            "timeout_s": 0.1,
        }
    )

    result = _wait_for_terminal_result(module, job_id)
    assert result["status"] == "timed_out"
    assert result["timed_out"] is True
    assert "timed out" in result["error"]


def test_command_runner_caps_results() -> None:
    module = CommandRunnerModule()
    module.start({"max_results": 1})

    first = module.submit_job({"command": sys.executable, "args": ["-c", "print('first')"]})
    second = module.submit_job({"command": sys.executable, "args": ["-c", "print('second')"]})

    _wait_for_terminal_result(module, second)
    results = module.list_job_results()
    assert len(results) == 1
    assert results[0]["job_id"] == second
    assert module.get_job_result(first) is None


def test_command_runner_rejects_invalid_payload() -> None:
    module = CommandRunnerModule()
    module.start({})

    with pytest.raises(ValueError, match="command"):
        module.submit_job({"args": []})

    with pytest.raises(ValueError, match="args"):
        module.submit_job({"command": sys.executable, "args": "not-a-list"})
