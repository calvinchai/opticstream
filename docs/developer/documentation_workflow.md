---
title: Documentation workflow
---

# Documentation workflow

The OpticStream documentation lives in the `docs/` directory and is built with
MkDocs. This page describes how contributors can build and preview the
documentation locally.

## Prerequisites

- A working Python environment (typically managed with
  [`uv`](https://docs.astral.sh/uv/); see the Installation guide for details).

## Install documentation dependencies

From the project root:

```bash
pip install "opticstream[dev]"
```

This installs MkDocs and any additional documentation-related dependencies
declared by the project.

## Working with external dependencies (linc-convert, nifti-zarr, psoct-toolbox)

The project depends on three external algorithm libraries that can be sourced
either from GitHub or from local clones:

- `linc-convert`
- `nifti-zarr`
- `psoct-toolbox`

In `pyproject.toml` these appear in two places:

- Under `[project.dependencies]` as Git-based requirements (GitHub URLs and
  branches/tags).
- Under `[tool.uv.sources]` as optional local editable paths, for example:

  ```toml
  [tool.uv.sources]
  linc-convert = { path = "/space/megaera/1/users/kchai/linc-convert", editable = true }
  nifti-zarr = { path = "/space/megaera/1/users/kchai/nifti-zarr", editable = true }
  psoct-toolbox = { path = "/space/megaera/1/users/kchai/code/psoct-toolbox", editable = true }
  ```

### Using local clones instead of GitHub

- **To work against local repositories**, ensure the three lines under
  `[tool.uv.sources]` in `pyproject.toml` are **uncommented** and point at your
  local checkout paths.
- When you then install or sync the environment with `uv` (for example
  `uv sync` or `uv pip install .`), `uv` will prefer these local source
  directories and install them in editable mode instead of pulling from GitHub.

If you comment out these `linc-convert`, `nifti-zarr`, and `psoct-toolbox`
entries, `uv` will fall back to the GitHub URLs defined in
`[project.dependencies]`.

### Debugging and repulling dependencies

The CLI exposes helper commands under the `utils` group to inspect and reset
these dependencies:

- Inspect current versions and where they are loaded from:

  ```bash
  opticstream utils inspect-deps          # all three
  opticstream utils inspect-deps -d nifti-zarr
  ```

  This prints, for each dependency, the installed version, the on-disk package
  path, whether it appears to be coming from a local editable checkout (as
  configured in `[tool.uv.sources]`), and the configured Git requirement.

- Repull dependencies from their GitHub repositories (ignoring local editable
  paths):

  ```bash
  opticstream utils repull-deps                    # all three
  opticstream utils repull-deps -d linc-convert    # only linc-convert
  opticstream utils repull-deps --dry-run          # show uv commands only
  ```

  Internally this uses `uv pip install --force-reinstall --no-deps <git-spec>`
  for each selected dependency, so it always repulls from the Git URLs defined
  in `pyproject.toml` regardless of any `[tool.uv.sources]` entries.

## Serve the documentation locally

From the project root:

```bash
mkdocs serve
```

By default this starts a local documentation site at
`http://127.0.0.1:8000/`, where you can browse getting started guides, user
guides, and API reference while you edit the Markdown files under `docs/`.

If a `mkdocs.yml` file is present in the repository root, it controls the
navigation structure and other MkDocs configuration for the site.

## Hosted documentation

End users should typically visit the hosted documentation site instead of
running MkDocs themselves. The README points to the current hosted URL.

