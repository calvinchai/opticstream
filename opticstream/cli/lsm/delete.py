from __future__ import annotations

from typing import Literal

from opticstream.cli.lsm.cli import lsm_cli
from opticstream.state.lsm_project_state import LSM_STATE_SERVICE

DeleteKind = Literal["slice", "channel", "strip"]


def _validate_delete_args(
    *,
    kind: DeleteKind,
    slice_id: int | None,
    channel_id: int | None,
    strip_id: int | None,
) -> None:
    if kind == "slice":
        if slice_id is None:
            raise ValueError("`--slice-id` is required when `--kind slice`.")
        return

    if kind == "channel":
        if slice_id is None:
            raise ValueError("`--slice-id` is required when `--kind channel`.")
        if channel_id is None:
            raise ValueError("`--channel-id` is required when `--kind channel`.")
        return

    if slice_id is None:
        raise ValueError("`--slice-id` is required when `--kind strip`.")
    if channel_id is None:
        raise ValueError("`--channel-id` is required when `--kind strip`.")
    if strip_id is None:
        raise ValueError("`--strip-id` is required when `--kind strip`.")


@lsm_cli.command
def delete(
    project_name: str,
    kind: DeleteKind,
    *,
    slice_id: int | None = None,
    channel_id: int | None = None,
    strip_id: int | None = None,
) -> None:
    """
    Delete a slice, channel, or strip from LSM project state.
    """
    _validate_delete_args(
        kind=kind,
        slice_id=slice_id,
        channel_id=channel_id,
        strip_id=strip_id,
    )

    with LSM_STATE_SERVICE.open_project_by_parts(project_name=project_name) as project:
        if kind == "slice":
            assert slice_id is not None
            deleted = project.delete_slice(slice_id)
            target = f"slice={slice_id}"
        elif kind == "channel":
            assert slice_id is not None
            assert channel_id is not None
            deleted = project.delete_channel(slice_id=slice_id, channel_id=channel_id)
            target = f"slice={slice_id}, channel={channel_id}"
        else:
            assert slice_id is not None
            assert channel_id is not None
            assert strip_id is not None
            deleted = project.delete_strip(
                slice_id=slice_id,
                channel_id=channel_id,
                strip_id=strip_id,
            )
            target = f"slice={slice_id}, channel={channel_id}, strip={strip_id}"

    if not deleted:
        raise ValueError(
            f"Target does not exist in project state: project={project_name!r}, kind={kind!r}, {target}"
        )

    print(f"Deleted {kind} from project={project_name!r}: {target}")
