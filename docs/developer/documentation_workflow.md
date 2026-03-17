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

