import sys

import pytest

from opticstream.utils.process_priority_thread import (
    process_name_matches,
    resolve_priority_flag,
)


@pytest.mark.parametrize(
    ("proc_name", "wanted", "expected"),
    [
        ("Foo.exe", "foo", True),
        ("foo.exe", "Foo", True),
        ("notepad.exe", "notepad.exe", True),
        ("python3", "python3", True),
        ("python", "python.exe", True),
        ("other.exe", "foo", False),
        (None, "foo", False),
    ],
)
def test_process_name_matches(
    proc_name: str | None, wanted: str, expected: bool
) -> None:
    assert process_name_matches(proc_name, wanted) is expected


def test_resolve_priority_flag_none() -> None:
    assert resolve_priority_flag(None) is None


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX nice values")
def test_resolve_priority_flag_posix() -> None:
    assert resolve_priority_flag("0") == 0
    assert resolve_priority_flag(" 5 ") == 5
    assert resolve_priority_flag("-10") == -10
    assert resolve_priority_flag("high") == -5
    assert resolve_priority_flag("NORMAL") == 0
    with pytest.raises(ValueError):
        resolve_priority_flag("not_an_int")
    with pytest.raises(ValueError):
        resolve_priority_flag("20")


@pytest.mark.skipif(sys.platform != "win32", reason="Windows priority class names")
def test_resolve_priority_flag_windows() -> None:
    import psutil

    assert resolve_priority_flag("below_normal") == psutil.BELOW_NORMAL_PRIORITY_CLASS
    assert resolve_priority_flag("NORMAL") == psutil.NORMAL_PRIORITY_CLASS
    with pytest.raises(ValueError):
        resolve_priority_flag("invalid")
