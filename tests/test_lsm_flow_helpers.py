"""Unit tests for LSM flow helpers: strip_process paths/utils, paths module, utils."""

import pytest

from opticstream.config.lsm_scan_config import LSMScanConfigModel
from opticstream.flows.lsm.paths import (
    channel_zarr_volume_path,
    strip_mip_output_path,
    strip_zarr_output_path,
)
from opticstream.flows.lsm.strip_process_flow import (
    build_status_message,
    compare_dir_manifests,
    format_bytes,
    invalid_path,
)
from opticstream.utils.zarr_validation import (
    DirManifest,
    get_dir_manifest,
    validate_zarr,
    validate_zarr_directory,
)
from opticstream.flows.lsm.utils import (
    channel_ident_from_payload,
    channel_ident_from_strip,
    strip_ident_from_payload,
)
from opticstream.state.lsm_project_state import LSMChannelId, LSMStripId


def _minimal_cfg(**kwargs) -> LSMScanConfigModel:
    base = dict(
        project_name="myproj",
        project_base_path="/base",
        info_file="/base/info.mat",
    )
    base.update(kwargs)
    return LSMScanConfigModel(**base)


def test_format_bytes_zero():
    assert "B" in format_bytes(0)


def test_format_bytes_kilobyte():
    assert "KB" in format_bytes(1024)


def test_build_status_message_success():
    status, msg = build_status_message(True, [], slice_id=3, strip_id=5)
    assert status == "success"
    assert "5" in msg and "3" in msg


def test_build_status_message_failure():
    status, msg = build_status_message(False, ["bad mip"], slice_id=1, strip_id=2)
    assert status == "error"
    assert "bad mip" in msg


@pytest.mark.parametrize(
    "path,expected",
    [
        (None, True),
        ("", True),
        ("/", True),
        (".", True),
        ("/tmp/out.tiff", False),
    ],
)
def test_invalid_path(path, expected):
    assert invalid_path(path) is expected


def test_get_dir_manifest_and_compare(tmp_path):
    f = tmp_path / "a.txt"
    f.write_text("hello")
    m = get_dir_manifest(str(tmp_path))
    assert m.file_count >= 1
    assert m.total_bytes >= 5
    assert compare_dir_manifests(m, m) is True
    m2 = DirManifest(file_count=0, total_bytes=0, sizes={})
    assert compare_dir_manifests(m, m2) is False


def test_strip_zarr_output_path_uses_project_base_when_output_none():
    cfg = _minimal_cfg(output_path=None)
    sid = LSMStripId(
        project_name="myproj", slice_id=2, strip_id=7, channel_id=1
    )
    p = strip_zarr_output_path(sid, cfg)
    assert p.startswith("/base")
    assert "slice02" in p or "02" in p
    assert "chunk-0007" in p or "0007" in p


def test_strip_zarr_output_path_prefers_output_path():
    cfg = _minimal_cfg(output_path="/out")
    sid = LSMStripId(
        project_name="myproj", slice_id=1, strip_id=1, channel_id=2
    )
    p = strip_zarr_output_path(sid, cfg)
    assert p.startswith("/out")


def test_strip_mip_output_path():
    cfg = _minimal_cfg(output_path="/z")
    sid = LSMStripId(
        project_name="myproj", slice_id=1, strip_id=3, channel_id=1
    )
    p = strip_mip_output_path(sid, cfg)
    assert p.startswith("/z")
    assert "proc-mip" in p


def test_channel_zarr_volume_path():
    cfg = _minimal_cfg()
    cid = LSMChannelId(project_name="myproj", slice_id=4, channel_id=2)
    p = channel_zarr_volume_path(cid, cfg)
    assert "myproj" in p
    assert "slice-04" in p
    assert "channel-02" in p
    assert p.endswith("_volume.zarr")


def test_strip_ident_from_payload_dict():
    sid = strip_ident_from_payload(
        {
            "strip_ident": {
                "project_name": "p",
                "slice_id": 1,
                "strip_id": 2,
                "channel_id": 3,
            }
        }
    )
    assert sid.project_name == "p"
    assert sid.strip_id == 2


def test_strip_ident_from_payload_missing():
    with pytest.raises(KeyError):
        strip_ident_from_payload({})


def test_channel_ident_from_payload_dict():
    cid = channel_ident_from_payload(
        {
            "channel_ident": {
                "project_name": "p",
                "slice_id": 1,
                "channel_id": 2,
            }
        }
    )
    assert cid.channel_id == 2


def test_channel_ident_from_strip():
    sid = LSMStripId(
        project_name="p", slice_id=1, strip_id=5, channel_id=2
    )
    cid = channel_ident_from_strip(sid)
    assert cid.project_name == "p"
    assert cid.slice_id == 1
    assert cid.channel_id == 2


def test_validate_zarr_directory_missing(tmp_path):
    class L:
        def error(self, *a, **k):
            pass

    r = validate_zarr_directory(
        L(),
        str(tmp_path / "nope"),
        100,
        context="ctx",
        missing_reason="missing",
        empty_reason="empty",
        below_threshold_reason="small",
    )
    assert not r.ok and r.reason == "missing"


def test_validate_zarr_directory_threshold_zero_nonempty(tmp_path):
    class L:
        def error(self, *a, **k):
            pass

        def info(self, *a, **k):
            pass

    (tmp_path / "f.txt").write_text("x")
    r = validate_zarr_directory(
        L(),
        str(tmp_path),
        0,
        context="c",
        missing_reason="m",
        empty_reason="e",
        below_threshold_reason="b",
    )
    assert r.ok and r.size_bytes >= 1


def test_validate_zarr_directory_below_threshold(tmp_path):
    class L:
        def error(self, *a, **k):
            pass

    (tmp_path / "tiny").write_text("ab")
    r = validate_zarr_directory(
        L(),
        str(tmp_path),
        1000,
        context="c",
        missing_reason="m",
        empty_reason="e",
        below_threshold_reason="too_small",
    )
    assert not r.ok and r.reason == "too_small"


def test_validate_zarr_wraps_directory_checks(tmp_path):
    class L:
        def error(self, *a, **k):
            pass

        def info(self, *a, **k):
            pass

    (tmp_path / "f.txt").write_text("x")
    r = validate_zarr(
        str(tmp_path),
        0,
        context="wrap",
        logger=L(),
    )
    assert r.ok and r.size_bytes >= 1

    r2 = validate_zarr(
        str(tmp_path / "missing.zarr"),
        100,
        context="wrap",
        logger=L(),
    )
    assert not r2.ok and r2.reason.startswith("wrap:")
