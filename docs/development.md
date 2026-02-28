# Development

## Repository Layout

- `src/sdsavior/ring.py`: core ring buffer implementation.
- `src/sdsavior/cli.py`: command-line interface.
- `tests/test_basic.py`: behavioral regression tests.
- `mkdocs.yml` and `docs/`: project documentation.

## Local Quality Checks

Run these before opening a PR:

```bash
ruff check .
mypy src
pytest -q
```

## Documentation Workflow

Preview docs while editing:

```bash
mkdocs serve
```

Build static docs:

```bash
mkdocs build
```
