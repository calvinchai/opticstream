## OpticStream

OpticStream provides Prefect-based workflows and utilities for processing large
microscopy and imaging datasets, including LSM strips and PS-OCT tiles.

The CLI is exposed as the `opticstream` command, with a short alias `ops`.

```bash
opticstream --help      # or: ops --help
opticstream deploy ...  # or: ops deploy ...
```

## Documentation

Full documentation is available at the hosted documentation site:

https://your-docs-url-here

If you want to build and browse the documentation locally while developing,
see the developer documentation under `docs/developer`.

## Quick start

### 1. Install and activate an environment (uv recommended)

We recommend using [`uv`](https://docs.astral.sh/uv/) to manage the development
environment for OpticStream.

```bash
git clone https://github.com/ORG/opticstream.git
cd opticstream

# create and sync a uv-managed environment from pyproject.toml / uv.lock
uv sync

# activate the virtual environment that uv created (optional)
source .venv/bin/activate  # or use `uv run` directly, see below
```

You can also use a traditional virtual environment and `pip install -e .` if
you prefer. See the Installation guide in the documentation site for full
details on uv, pip, and pipx based installs.

### 2. Explore the CLI

```bash
opticstream --help      # or: ops --help
opticstream deploy --help
```

### 3. Follow the full quickstart

For a complete walkthrough (including configuring scan blocks and running an
example LSM strip processing flow), see the Quickstart in the documentation
site (`docs/getting-started/quickstart.md` when served with MkDocs).

## Note about git-based dev dependencies

OpticStream currently depends on several git-based repositories for development:

- `linc-convert` (branch `lsm-features`)
- `nifti-zarr` (branch `port-multizarr`)
- `psoct-toolbox`

These are referenced in `pyproject.toml` via explicit git URLs (and optionally in
`[tool.uv.sources]` for local development overrides).

**Maintainers:** whenever any of the following happens, you must review and update
these references:

- The `opticstream` repository is transferred to a different organization or
  GitHub namespace.
- The branches used in these dependency URLs are renamed, merged, or deleted.
- The project is prepared for, or published to, PyPI and you switch to using
  PyPI-hosted versions of these packages.

In those cases, check and update:

- The git URLs and branch names in `[project.dependencies]` in `pyproject.toml`.
- Any `[tool.uv.sources]` entries in `pyproject.toml` (for example `git = ...`,
  `branch = ...`, or `path = ...` values).

Avoid committing hard-coded, machine-specific paths (such as `path = "/home/username/..."`)
that may not exist on other systems. Failing to update these references can cause installs
from git or PyPI to break or to pull the wrong versions of the dev dependencies.

