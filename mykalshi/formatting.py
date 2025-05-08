from datetime import datetime
import numpy as np


def parse_timestamp(ts):
    """Convert MM/DD/YYYY [HH:MM:SS] to Unix timestamp (int)."""
    if isinstance(ts, str):
        for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"):
            try:
                return int(datetime.strptime(ts, fmt).timestamp())
            except ValueError:
                continue
        raise ValueError(f"Invalid timestamp format: {ts}")
    return ts  # assume already in correct format if not string

def format_timestamp(ts):
    """Convert Unix timestamp (int) to MM/DD/YYYY HH:MM:SS."""
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts).strftime("%m/%d/%Y %H:%M:%S")
    elif isinstance(ts, str):
        try:
            ts = float(ts)
            return datetime.fromtimestamp(ts).strftime("%m/%d/%Y %H:%M:%S")
        except ValueError:
            raise ValueError(f"Invalid timestamp: {ts}")
    else:
        raise TypeError(f"Unsupported type for timestamp: {type(ts)}")
    
def parse_range_string(s):
    s = s.replace(',', '')  # remove commas
    if "or lower" in s or "or below" in s:
        max_val = float(s.split(" ")[0])
        return (0.0, max_val)
    elif "or higher" in s or "or above" in s:
        min_val = float(s.split(" ")[0])
        return (min_val, np.inf)
    elif "to" in s:
        parts = s.split(" to ")
        return (float(parts[0]), float(parts[1]))
    else:
        raise ValueError(f"Unrecognized format: {s}")