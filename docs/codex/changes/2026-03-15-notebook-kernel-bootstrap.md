# 2026-03-15 Notebook Kernel Bootstrap

## Summary

Added a kernel/bootstrap guard to `notebooks/main_current.ipynb` so notebook failures surface as an explicit environment/setup instruction instead of a raw `ModuleNotFoundError`.

## Behavior

- resolves the repo root from the notebook working directory
- inserts the repo root onto `sys.path` when needed
- checks for `matplotlib`, `pandas`, and `seaborn`
- raises a clear message with the exact `%pip install -e "...[analysis,storage,websocket]"` command to run in the current notebook kernel

## Why

The notebook code itself was valid, but VS Code/Jupyter can run it under a kernel that is not the repo `.venv`. In that case, optional analysis dependencies are missing even though the repo environment is correctly configured.
