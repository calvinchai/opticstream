# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for OpticNode Windows binary. Run from repo root:
    uv sync --extra dev && uv run pyinstaller opticnode/opticnode.spec
"""
from __future__ import annotations

from pathlib import Path

from PyInstaller.building.build_main import Analysis
from PyInstaller.building.api import EXE, PYZ
from PyInstaller.utils.hooks import collect_all, collect_submodules

block_cipher = None

REPO_ROOT = Path(".").resolve()
OPTIC = REPO_ROOT / "opticnode"

grpc_datas, grpc_binaries, grpc_hidden = collect_all("grpc")

datas = list(grpc_datas)
binaries = list(grpc_binaries)

hiddenimports = list(grpc_hidden) + [
    "grpc._cython.cygrpc",
    "google.protobuf",
    "google.protobuf.pyext._message",
    "watchdog.observers",
    "watchdog.observers.polling",
    "watchdog.observers.read_directory_changes",
    "watchdog.observers.winapi",
    "pystray._base",
    "pystray._win32",
    "PIL._imaging",
    "redis",
    "requests",
    "packaging.version",
    "urllib3",
    "certifi",
]

hiddenimports += collect_submodules("opticnode")
hiddenimports += collect_submodules("opticapi")

a = Analysis(
    [str(OPTIC / "bootstrap_exe.py")],
    pathex=[str(REPO_ROOT)],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["matplotlib.tests", "numpy.tests"],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name="opticnode",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
