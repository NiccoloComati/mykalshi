# Flow Script Fixes

Date: 2026-03-15

## Summary

This slice cleaned up the local flow scripts after manual terminal testing exposed two usability problems.

## Main Changes

- updated `tar_flow.py` to:
  - stop using a dead placeholder
  - accept an optional `series_ticker` CLI argument
  - default to the first open event when no argument is passed
- updated `his_flow.py` to:
  - accept an optional historical ticker CLI argument
  - default to the first available archived ticker
- improved the missing-`requests` dependency error in `mykalshi/client.py`

## Verification

- `.\.venv\Scripts\python tar_flow.py` passed
- `.\.venv\Scripts\python his_flow.py` passed
- plain `python tar_flow.py` now fails with an actionable dependency message instead of a vague one
