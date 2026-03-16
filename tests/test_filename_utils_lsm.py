import os.path as op

import pytest

from opticstream.utils.filename_utils import (
    parse_lsm_run_folder_name,
    parse_lsm_strip_index,
    parse_lsm_strip_index_from_filename,
)


@pytest.mark.parametrize(
    "folder_name,expected",
    [
        ("Run1", (1, 1, 1)),
        ("Run1_1", (1, 2, 1)),
        ("Run1_4", (1, 5, 1)),
        ("Run1_C2", (1, 1, 2)),
        ("Run1_C2_3", (1, 4, 2)),
        ("run10", (10, 1, 1)),
        ("RUN10_C2_5", (10, 6, 2)),
        (op.join("/some/path", "Run2_3"), (2, 4, 1)),
        (op.join("/some/path", "Run2_3") + op.sep, (2, 4, 1)),
    ],
)
def test_parse_lsm_run_folder_name_valid_cases(folder_name, expected):
    assert parse_lsm_run_folder_name(folder_name) == expected


@pytest.mark.parametrize(
    "folder_name",
    [
        "Foo",
        "Run",
        "Run_1",
        "Run1_C3",
        "Run1__2",
        "Run1_C2_extra",
        "",
    ],
)
def test_parse_lsm_run_folder_name_invalid_cases(folder_name):
    with pytest.raises(ValueError) as excinfo:
        parse_lsm_run_folder_name(folder_name)
    msg = str(excinfo.value)
    assert "Folder name does not match LSM run pattern" in msg


@pytest.mark.parametrize(
    "strip_index,channel_index,strips_per_slice,expected",
    [
        (1, 1, 5, (1, 1, 1)),
        (5, 1, 5, (1, 5, 1)),
        (6, 2, 5, (2, 1, 2)),
        (7, 2, 5, (2, 2, 2)),
        (10, 1, 5, (2, 5, 1)),
    ],
)
def test_parse_lsm_strip_index(strip_index, channel_index, strips_per_slice, expected):
    assert parse_lsm_strip_index(strip_index, channel_index, strips_per_slice) == expected


@pytest.mark.parametrize(
    "folder_name,strips_per_slice,expected",
    [
        ("Run1", 5, (1, 1, 1)),
        ("Run1_3", 5, (1, 4, 1)),
        ("Run1_C2_4", 5, (1, 5, 2)),
        (op.join("/root/path", "Run1_C2_3"), 5, (1, 4, 2)),
    ],
)
def test_parse_lsm_strip_index_from_filename_composition_and_basename_handling(
    folder_name, strips_per_slice, expected
):
    slice_index, strip_index, channel_index = parse_lsm_strip_index_from_filename(
        folder_name, strips_per_slice
    )
    assert (slice_index, strip_index, channel_index) == expected

