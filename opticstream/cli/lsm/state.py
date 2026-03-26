from __future__ import annotations

from typing import Literal

from cyclopts import App
from opticstream.cli.state_common import (
    apply_mark_field,
    apply_mark_status,
    validate_mark_field_and_value,
)
from opticstream.cli.lsm.cli import lsm_cli
from opticstream.state.lsm_project_state import (
    LSMChannelState,
    LSMSliceState,
    LSMStripState,
    LSM_STATE_SERVICE,
)

lsm_state_cli = lsm_cli.command(App(name="state"))


@lsm_cli.command
def reset(
    project_name: str,
    *,
    slice: int | None = None,
    channel: int | None = None,
    strip: int | None = None,
) -> None:
    """
    Delete a slice, channel, or strip from LSM project state.

    Examples:
    - opticstream lsm reset myproject --slice 1
    - opticstream lsm reset myproject --slice 1 --channel 2
    - opticstream lsm reset myproject --slice 1 --channel 2 --strip 3
    """
    if slice is None:
        raise ValueError("`--slice` is required.")
    if strip is not None and channel is None:
        raise ValueError("`--channel` is required when `--strip` is provided.")

    with LSM_STATE_SERVICE.open_project_by_parts(project_name=project_name) as project:
        if channel is None:
            deleted = project.delete_slice(slice)
            target = f"slice={slice}"
            hierarchy = "slice"
        elif strip is None:
            deleted = project.delete_channel(slice_id=slice, channel_id=channel)
            target = f"slice={slice}, channel={channel}"
            hierarchy = "channel"
        else:
            deleted = project.delete_strip(
                slice_id=slice,
                channel_id=channel,
                strip_id=strip,
            )
            target = f"slice={slice}, channel={channel}, strip={strip}"
            hierarchy = "strip"

    if not deleted:
        raise ValueError(
            "Target does not exist in project state: "
            f"project={project_name!r}, hierarchy={hierarchy!r}, {target}"
        )

    print(f"Reset {hierarchy} from project={project_name!r}: {target}")


MarkHierarchy = Literal["slice", "channel", "strip"]


@lsm_state_cli.command
def mark(
    project_name: str,
    hierarchy: MarkHierarchy,
    field: str,
    value: str | None = None,
    *,
    slice: int | None = None,
    channel: int | None = None,
    strip: int | None = None,
) -> None:
    """
    Set LSM state values for a hierarchy target.

    Examples:
    - opticstream lsm state mark myproject channel completed --slice 1
    - opticstream lsm state mark myproject channel volume_uploaded true --slice 1
    - opticstream lsm state mark myproject slice completed --slice 1
    """
    if slice is None:
        raise ValueError("`--slice` is required.")
    if hierarchy == "strip" and channel is None:
        raise ValueError("`--channel` is required for `strip` hierarchy.")

    is_status = validate_mark_field_and_value(field, value)

    updated = 0
    with LSM_STATE_SERVICE.open_project_by_parts(project_name=project_name) as project:
        targets: list[LSMSliceState | LSMChannelState | LSMStripState] = []
        if hierarchy == "slice":
            slice_state = project.slices.get(slice)
            if slice_state is None:
                raise ValueError(f"Slice not found: slice={slice}")
            targets = [slice_state]
        elif hierarchy == "channel":
            slice_state = project.slices.get(slice)
            if slice_state is None:
                raise ValueError(f"Slice not found: slice={slice}")
            if channel is None:
                targets = list(slice_state.channels.values())
                if not targets:
                    raise ValueError(f"No channels found in slice={slice}")
            else:
                channel_state = slice_state.channels.get(channel)
                if channel_state is None:
                    raise ValueError(f"Channel not found: slice={slice}, channel={channel}")
                targets = [channel_state]
        else:
            assert channel is not None
            slice_state = project.slices.get(slice)
            if slice_state is None:
                raise ValueError(f"Slice not found: slice={slice}")
            channel_state = slice_state.channels.get(channel)
            if channel_state is None:
                raise ValueError(f"Channel not found: slice={slice}, channel={channel}")
            if strip is None:
                targets = list(channel_state.strips.values())
                if not targets:
                    raise ValueError(f"No strips found: slice={slice}, channel={channel}")
            else:
                strip_state = channel_state.strips.get(strip)
                if strip_state is None:
                    raise ValueError(
                        f"Strip not found: slice={slice}, channel={channel}, strip={strip}"
                    )
                targets = [strip_state]

        for target in targets:
            if is_status:
                apply_mark_status(target, field)  # type: ignore[arg-type]
            else:
                assert value is not None
                apply_mark_field(target, field, value)
            updated += 1

    print(
        f"Updated {updated} {hierarchy}(s) in project={project_name!r}: "
        f"field={field!r}, value={value!r}, slice={slice}, channel={channel}, strip={strip}"
    )
