"""Public package interface."""

from __future__ import annotations

from .detector import GenderDetect
from .normalization import normalize_names

__version__: str


def __getattr__(name: str) -> str:
    """Load module attributes that are derived from package metadata."""
    if name == "__version__":
        from importlib.metadata import version

        value = version("mysoc-gender-detect")
        globals()[name] = value
        return value

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["GenderDetect", "normalize_names"]
