from __future__ import annotations

"""
Utility-related commands for the opticstream CLI.

This module provides helper commands under the `utils` subgroup, including
remote deployment helpers that can launch and manage deployment processes on
remote hosts via SSH.
"""

from __future__ import annotations

import shlex
import subprocess
from dataclasses import dataclass
from typing import Iterable, List, Sequence

from .root import utils


@dataclass(frozen=True)
class RemoteConnection:
    """
    Connection parameters for a remote host accessed via SSH.

    Attributes
    ----------
    host:
        Hostname or IP address of the remote machine.
    user:
        Optional username. If provided, connections use `user@host`.
    port:
        Optional SSH port. If provided, passed via `-p`.
    """

    host: str
    user: str | None = None
    port: int | None = None


def _build_ssh_target(conn: RemoteConnection) -> str:
    if conn.user:
        return f"{conn.user}@{conn.host}"
    return conn.host


def _build_ssh_command(conn: RemoteConnection, remote_shell_command: str) -> List[str]:
    """
    Build the base `ssh` command list that will execute a shell command remotely.
    """
    cmd: List[str] = ["ssh"]
    if conn.port is not None:
        cmd.extend(["-p", str(conn.port)])
    cmd.append(_build_ssh_target(conn))
    cmd.append(remote_shell_command)
    return cmd


def _quote_args(args: Iterable[str]) -> str:
    """
    Safely quote arguments for inclusion in a remote shell command.
    """
    return " ".join(shlex.quote(a) for a in args)


def _build_remote_deploy_command(
    *,
    remote_dir: str | None,
    base_command: Sequence[str],
    deploy_tag: str,
    log_file: str,
    use_tmux: bool,
    tmux_session_prefix: str = "optic-deploy",
) -> str:
    """
    Build the remote shell snippet that runs the deployment in nohup or tmux.

    The command will:
    - Optionally `cd` into `remote_dir`.
    - Append `--optic-deploy-tag=<tag>` to the base command.
    - In nohup mode, run under `nohup`, redirecting output to `log_file`, and
      backgrounding the process while echoing its PID.
    - In tmux mode, create a detached tmux session with a predictable name and
      run the command there, logging to `log_file`.
    """
    tag_arg = f"--optic-deploy-tag={deploy_tag}"
    full_cmd: List[str] = list(base_command) + [tag_arg]
    quoted_cmd = _quote_args(full_cmd) + f" > {shlex.quote(log_file)} 2>&1"

    parts: List[str] = []
    if remote_dir:
        parts.append(f"cd {shlex.quote(remote_dir)}")

    if use_tmux:
        session_name = f"{tmux_session_prefix}-{deploy_tag}"
        tmux_cmd = (
            f"tmux new -d -s {shlex.quote(session_name)} {shlex.quote(quoted_cmd)}"
        )
        parts.append(tmux_cmd)
    else:
        nohup_cmd = f"nohup {quoted_cmd} & echo $!"
        parts.append(nohup_cmd)

    return " && ".join(parts)


def _build_remote_kill_command(
    *,
    include_nohup: bool,
    include_tmux: bool,
    tag: str | None,
    tmux_session_prefix: str = "optic-deploy",
) -> str:
    """
    Build a remote shell snippet that kills deployments started by this tool.

    The command targets:
    - Tmux sessions whose names match `tmux_session_prefix-<tag>` (or all
      `tmux_session_prefix-*` when `tag` is None), and/or
    - Processes whose command line includes `--optic-deploy-tag=<tag>` (or
      `--optic-deploy-tag=` when `tag` is None).
    """
    subcommands: List[str] = []

    if include_tmux:
        if tag is not None:
            pattern = f"{tmux_session_prefix}-{tag}"
        else:
            pattern = f"{tmux_session_prefix}-"
        tmux_kill = (
            "tmux ls 2>/dev/null | "
            f"grep {shlex.quote(pattern)} || true; "
            "tmux ls 2>/dev/null | "
            f"grep {shlex.quote(pattern)} | "
            "cut -d: -f1 | "
            "xargs -r -n1 tmux kill-session -t"
        )
        subcommands.append(f"({tmux_kill} || true)")

    if include_nohup:
        if tag is not None:
            grep_pattern = f"--optic-deploy-tag={tag}"
        else:
            grep_pattern = "--optic-deploy-tag="
        ps_kill = (
            "ps aux | "
            f"grep {shlex.quote(grep_pattern)} | "
            "grep -v grep | "
            "awk '{print $2}' | "
            "xargs -r kill"
        )
        subcommands.append(f"({ps_kill} || true)")

    if not subcommands:
        # Nothing to do; return a no-op that still succeeds.
        return "true"

    return " && ".join(subcommands)


@utils.command
def remote_deploy(
    host: str,
    *,
    script: str | None = None,
    command: str | None = None,
    user: str | None = None,
    port: int | None = None,
    remote_dir: str | None = None,
    log_file: str = "~/optic-deploy.log",
    tag: str | None = None,
    use_tmux: bool = False,
    tmux_session_prefix: str = "optic-deploy",
    dry_run: bool = False,
    extra_args: Sequence[str] | None = None,
) -> None:
    """
    Run a deployment command on a remote host via SSH.

    By default, this uses `nohup` to run the deployment in the background so
    it survives SSH disconnects. When `use_tmux` is True, the deployment is
    instead launched in a detached tmux session so you can reattach later.

    Exactly one of `script` or `command` must be provided. Any `extra_args`
    are appended to the invocation. A deployment tag is automatically added
    via `--optic-deploy-tag=<tag>` so that `remote-deploy-kill` can later
    terminate matching processes or tmux sessions.
    """
    if (script is None) == (command is None):
        raise ValueError("Exactly one of `script` or `command` must be provided.")

    if tag is None:
        if script is not None:
            tag = script.replace("/", "_")
        else:
            tag = "deploy"

    if extra_args is None:
        extra_args = ()

    if script is not None:
        base_command: List[str] = [script, *extra_args]
    else:
        # When a full command string is provided, wrap it in `sh -c` so we can
        # still append the tag argument in a controlled way.
        base_command = ["sh", "-c", command, "--", *extra_args]

    conn = RemoteConnection(host=host, user=user, port=port)
    remote_shell = _build_remote_deploy_command(
        remote_dir=remote_dir,
        base_command=base_command,
        deploy_tag=tag,
        log_file=log_file,
        use_tmux=use_tmux,
        tmux_session_prefix=tmux_session_prefix,
    )
    ssh_cmd = _build_ssh_command(conn, remote_shell)

    if dry_run:
        print("Remote deploy (dry run):")
        print(" ".join(shlex.quote(part) for part in ssh_cmd))
        return

    completed = subprocess.run(ssh_cmd, check=False, text=True, capture_output=True)
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="")


@utils.command
def remote_deploy_kill(
    host: str,
    *,
    user: str | None = None,
    port: int | None = None,
    include_nohup: bool = True,
    include_tmux: bool = True,
    tag: str | None = None,
    tmux_session_prefix: str = "optic-deploy",
    dry_run: bool = False,
) -> None:
    """
    Kill all remote deployments started by `remote-deploy` on the given host.

    This command connects to the remote host via SSH and:
    - Optionally kills tmux sessions whose names start with the tmux session
      prefix (default: `optic-deploy`) and match the given tag.
    - Optionally kills processes whose command line contains
      `--optic-deploy-tag=<tag>` (or any `--optic-deploy-tag=` when tag is
      omitted).
    """
    conn = RemoteConnection(host=host, user=user, port=port)
    remote_shell = _build_remote_kill_command(
        include_nohup=include_nohup,
        include_tmux=include_tmux,
        tag=tag,
        tmux_session_prefix=tmux_session_prefix,
    )
    ssh_cmd = _build_ssh_command(conn, remote_shell)

    if dry_run:
        print("Remote deploy kill (dry run):")
        print(" ".join(shlex.quote(part) for part in ssh_cmd))
        return

    completed = subprocess.run(ssh_cmd, check=False, text=True, capture_output=True)
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="")
