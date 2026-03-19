from opticstream.tasks.dandi_upload import build_dandi_upload_command


def test_build_dandi_upload_command_defaults_to_dandi_instance() -> None:
    cmd, _, _ = build_dandi_upload_command(
        ["/tmp/file1.nii", "/tmp/file2.nii"],
        dandi_instance="dandi",
        realpath=False,
    )
    assert "-i" not in cmd
    assert "/tmp/file1.nii" in cmd
    assert "/tmp/file2.nii" in cmd


def test_build_dandi_upload_command_includes_linc_instance_flag() -> None:
    cmd, _, _ = build_dandi_upload_command(
        ["/tmp/file1.nii", "/tmp/file2.nii"],
        dandi_instance="linc",
        realpath=False,
    )
    assert "-i linc" in cmd
    assert "/tmp/file1.nii" in cmd
    assert "/tmp/file2.nii" in cmd

