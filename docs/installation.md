# Installation

## Runtime Install

```bash
pip install sdsavior
```

## Development Install

Install in editable mode with tooling:

```bash
pip install -e ".[dev]"
```

This includes:

- `pytest` for tests
- `ruff` for linting
- `mypy` for type checking
- `mkdocs` for docs build/serve

## Build Docs Locally

Serve docs with live reload:

```bash
mkdocs serve
```

Create a static site:

```bash
mkdocs build
```
