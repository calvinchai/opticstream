import pytest

from opticstream.cli.lsm.delete import _validate_delete_args


def test_validate_delete_args_accepts_required_combinations():
    _validate_delete_args(
        kind="slice",
        slice_id=1,
        channel_id=None,
        strip_id=None,
    )
    _validate_delete_args(
        kind="channel",
        slice_id=1,
        channel_id=2,
        strip_id=None,
    )
    _validate_delete_args(
        kind="strip",
        slice_id=1,
        channel_id=2,
        strip_id=3,
    )


@pytest.mark.parametrize(
    ("kind", "slice_id", "channel_id", "strip_id", "expected_message"),
    [
        ("slice", None, None, None, "`--slice-id` is required when `--kind slice`."),
        (
            "channel",
            None,
            None,
            None,
            "`--slice-id` is required when `--kind channel`.",
        ),
        ("channel", 1, None, None, "`--channel-id` is required when `--kind channel`."),
        ("strip", None, None, None, "`--slice-id` is required when `--kind strip`."),
        ("strip", 1, None, None, "`--channel-id` is required when `--kind strip`."),
        ("strip", 1, 2, None, "`--strip-id` is required when `--kind strip`."),
    ],
)
def test_validate_delete_args_rejects_missing_required_ids(
    kind: str,
    slice_id: int | None,
    channel_id: int | None,
    strip_id: int | None,
    expected_message: str,
):
    with pytest.raises(ValueError, match=expected_message):
        _validate_delete_args(
            kind=kind,  # type: ignore[arg-type]
            slice_id=slice_id,
            channel_id=channel_id,
            strip_id=strip_id,
        )
