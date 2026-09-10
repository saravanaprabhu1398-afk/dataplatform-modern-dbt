"""Duration strings for configuration.

Windowing configuration reads far better as ``6h`` than as ``21600``, and a
reader who has to count zeros is a reader who will eventually miscount them.
Accepts a plain number of seconds, or a number with a unit suffix:

    30s   90m   6h   2d

Lives in ``core`` because both the config models and the streaming runtime
need it, and neither should depend on the other.
"""
from __future__ import annotations

import re
from typing import Union

_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([smhd]?)\s*$", re.IGNORECASE)

_MULTIPLIERS = {"": 1, "s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(value: Union[str, int, float]) -> int:
    """Return *value* in whole seconds.

    Raises ``ValueError`` on anything unparseable rather than guessing -- a
    silently misread window size is a wrong answer that looks right.
    """
    if isinstance(value, bool):
        raise ValueError("duration must be a number or string, not a bool")
    if isinstance(value, (int, float)):
        seconds = float(value)
    else:
        match = _PATTERN.match(str(value))
        if not match:
            raise ValueError(
                "invalid duration {0!r}; expected e.g. 30s, 90m, 6h, 2d".format(value)
            )
        seconds = float(match.group(1)) * _MULTIPLIERS[match.group(2).lower()]

    if seconds < 0:
        raise ValueError("duration must not be negative: {0!r}".format(value))
    return int(seconds)
