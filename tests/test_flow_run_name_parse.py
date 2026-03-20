from opticstream.utils.flow_run_name_parse import (
    missing_required_fields,
    parse_flow_run_name_fields,
)


def test_parse_project_only():
    parsed = parse_flow_run_name_fields("project_name='111'")
    assert parsed == {"project_name": "111"}


def test_parse_project_and_slice():
    parsed = parse_flow_run_name_fields("project_name='111' slice_id=1")
    assert parsed == {"project_name": "111", "slice_id": 1}


def test_parse_project_slice_and_mosaic():
    parsed = parse_flow_run_name_fields("project_name='111' slice_id=1 mosaic_id=4")
    assert parsed == {"project_name": "111", "slice_id": 1, "mosaic_id": 4}


def test_parse_with_prefix_and_extra_fields():
    parsed = parse_flow_run_name_fields(
        "process-mosaic-project_name='111' slice_id=1 mosaic_id=4 some_flag='x'"
    )
    assert parsed["project_name"] == "111"
    assert parsed["slice_id"] == 1
    assert parsed["mosaic_id"] == 4
    assert parsed["some_flag"] == "x"


def test_parse_lsm_ident_fields():
    parsed = parse_flow_run_name_fields("project_name='111' slice_id=1 channel_id=1 strip_id=1")
    assert parsed == {
        "project_name": "111",
        "slice_id": 1,
        "channel_id": 1,
        "strip_id": 1,
    }


def test_missing_required_fields():
    parsed = parse_flow_run_name_fields("project_name='111' mosaic_id=4")
    assert missing_required_fields(parsed, ("project_name", "slice_id", "mosaic_id")) == ["slice_id"]
