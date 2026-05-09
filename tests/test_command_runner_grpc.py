from __future__ import annotations

import sys
import time
from concurrent import futures
from types import SimpleNamespace

import grpc
import pytest

from opticnode.generated import command_runner_pb2 as crpb2
from opticnode.generated.command_runner_pb2_grpc import (
    CommandRunnerStub,
    add_CommandRunnerServicer_to_server,
)
from opticnode.modules.base import ModuleRegistry
from opticnode.modules.command_runner import CommandRunnerModule
from opticnode.servicer.command_runner_rpc import CommandRunnerServicer


@pytest.fixture
def grpc_command_runner():
    settings = SimpleNamespace(node_id="test-node", redis_url="redis://localhost:0/0")
    registry = ModuleRegistry(settings, log_buffer=None)
    registry._redis = None  # disable persistence for the test
    registry.register_factory("command_runner", CommandRunnerModule)
    registry.start("command_runner", {"max_results": 10})

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    add_CommandRunnerServicer_to_server(CommandRunnerServicer(registry), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield CommandRunnerStub(channel)
    finally:
        channel.close()
        server.stop(None)
        registry.shutdown_all()


def _wait_for_terminal(stub: CommandRunnerStub, job_id: str) -> crpb2.CommandJobResult:
    deadline = time.time() + 5.0
    while time.time() < deadline:
        resp = stub.GetCommandResult(crpb2.GetCommandResultRequest(job_id=job_id))
        assert resp.ok, resp.error
        if resp.result.status in {"finished", "failed", "timed_out"}:
            return resp.result
        time.sleep(0.02)
    raise AssertionError("job did not finish in time")


def test_submit_and_get_command_result(grpc_command_runner: CommandRunnerStub) -> None:
    submit = grpc_command_runner.SubmitCommand(
        crpb2.SubmitCommandRequest(
            command=sys.executable,
            args=["-c", "print('hello')"],
            timeout_s=5.0,
        )
    )
    assert submit.ok and submit.job_id

    result = _wait_for_terminal(grpc_command_runner, submit.job_id)
    assert result.status == "finished"
    assert result.exit_code == 0
    assert result.stdout == "hello\n"


def test_list_command_results(grpc_command_runner: CommandRunnerStub) -> None:
    job_ids = []
    for value in ("first", "second"):
        resp = grpc_command_runner.SubmitCommand(
            crpb2.SubmitCommandRequest(
                command=sys.executable,
                args=["-c", f"print({value!r})"],
                timeout_s=5.0,
            )
        )
        assert resp.ok
        job_ids.append(resp.job_id)
        _wait_for_terminal(grpc_command_runner, resp.job_id)

    listing = grpc_command_runner.ListCommandResults(crpb2.ListCommandResultsRequest(limit=10))
    listed_ids = [r.job_id for r in listing.results]
    assert listed_ids[0] == job_ids[-1]
    assert set(job_ids).issubset(listed_ids)


def test_submit_rejects_empty_command(grpc_command_runner: CommandRunnerStub) -> None:
    with pytest.raises(grpc.RpcError) as exc_info:
        grpc_command_runner.SubmitCommand(crpb2.SubmitCommandRequest(command=""))
    assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT
