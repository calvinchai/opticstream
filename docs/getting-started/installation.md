---
title: Installation
---

# Installation

## Requirements

- Python 3.11+ (matches `requires-python` in `pyproject.toml`)
- [`uv`](https://docs.astral.sh/uv/) (recommended) or a Python virtual environment tool (e.g. `venv`, `conda`)

## Install with uv (recommended)

`uv` is the recommended way to create and manage a development environment for
OpticStream. It reads dependencies from `pyproject.toml` (and `uv.lock` if
present) and creates a virtual environment for you.

```bash
git clone https://github.com/ORG/opticstream.git
cd opticstream

# create and sync environment from pyproject.toml / uv.lock
uv sync

# optional: activate the environment that uv created
source .venv/bin/activate
```

Once synced, you can run the CLI via the environment:

```bash
opticstream --help      # or: ops --help
```

You can also run commands without explicitly activating the environment by using
`uv run`:

```bash
uv run opticstream --help
```

## Alternative: install with pip

If you prefer to use `pip` directly, you can install from source into a virtual
environment:

```bash
git clone https://github.com/ORG/opticstream.git
cd opticstream
python -m venv .venv
source .venv/bin/activate

pip install -e .
```

This installs `opticstream` in editable mode so local code changes are picked up
immediately.

## Alternative: install with pipx

For a more isolated, globally-available installation, you can use `pipx`:

```bash
git clone https://github.com/ORG/opticstream.git
cd opticstream
pipx install .
```

This makes the `opticstream` (and `ops`) CLI available on your PATH while
keeping dependencies isolated. For active development on the codebase, uv or a
local virtual environment is still recommended.

## Next steps

After installation, continue with the Quickstart guide to configure a project
and run an example flow (see `docs/getting-started/quickstart.md` in the
documentation site).

