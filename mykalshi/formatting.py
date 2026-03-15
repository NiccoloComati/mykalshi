from __future__ import annotations

from datetime import datetime


def parse_timestamp(ts):
    """Convert MM/DD/YYYY [HH:MM[:SS]] or Unix-like values to integer seconds."""
    if isinstance(ts, str):
        for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
            try:
                return int(datetime.strptime(ts, fmt).timestamp())
            except ValueError:
                continue
        raise ValueError(f"Invalid timestamp format: {ts}")
    return ts


def format_timestamp(ts):
    """Convert Unix timestamp values to MM/DD/YYYY HH:MM:SS."""
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts).strftime("%m/%d/%Y %H:%M:%S")
    if isinstance(ts, str):
        try:
            return datetime.fromtimestamp(float(ts)).strftime("%m/%d/%Y %H:%M:%S")
        except ValueError as exc:
            raise ValueError(f"Invalid timestamp: {ts}") from exc
    raise TypeError(f"Unsupported type for timestamp: {type(ts)}")


def parse_range_string(value):
    try:
        import numpy as np
    except ImportError as exc:
        raise ImportError("numpy is required for parse_range_string") from exc

    normalized = value.replace(",", "")
    if "or lower" in normalized or "or below" in normalized:
        max_value = float(normalized.split(" ")[0])
        return (0.0, max_value)
    if "or higher" in normalized or "or above" in normalized:
        min_value = float(normalized.split(" ")[0])
        return (min_value, np.inf)
    if "to" in normalized:
        lower, upper = normalized.split(" to ")
        return (float(lower), float(upper))
    raise ValueError(f"Unrecognized format: {value}")
