@echo off
setlocal EnableExtensions

rem Repo root (this script's directory)
cd /d "%~dp0"

where uv >nul 2>&1
if not errorlevel 1 goto :with_uv

if exist ".venv\Scripts\opticnode.exe" (
    call ".venv\Scripts\opticnode.exe" --gui
    goto :finish
)

echo.
echo Could not start OpticNode.
echo   Install uv: https://docs.astral.sh/uv/getting-started/installation/
echo   Then double-click this file again (first run runs: uv sync).
echo.
goto :fail

:with_uv
if not exist ".venv\" (
    echo First run: installing dependencies with uv sync...
    uv sync
    if errorlevel 1 goto :fail
)
uv run opticnode --gui
goto :finish

:finish
if errorlevel 1 goto :fail
exit /b 0

:fail
pause
exit /b 1
