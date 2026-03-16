# Notebook Kernel Fix

Date: 2026-03-15

## What Changed

- added `ipykernel` to the `analysis` optional dependency in `pyproject.toml`
- updated `notebooks/main_current.ipynb` to target a dedicated `mykalshi (.venv)` kernelspec
- clarified the notebook bootstrap error message so it points users at the explicit `mykalshi (.venv)` kernel name

## Why

The previous notebook update improved missing-package diagnostics, but it still depended on the editor picking the repo virtual environment correctly. On a machine where the notebook was opened under a different kernel, that looked like the notebook environment itself was broken. This fix makes the repo's intended notebook environment explicit and installable.

## Verification

- `.\\.venv\\Scripts\\python -m pip show ipykernel`
- `.\\.venv\\Scripts\\python -m jupyter kernelspec list`
- notebook metadata check for `notebooks/main_current.ipynb`
