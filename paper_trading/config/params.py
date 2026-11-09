"""
Parameter loader — single entry point for every tuned value in the system.
==========================================================================

All thresholds, periods, multipliers, intervals, sizing and friction values
are read from a YAML file instead of being hard-coded:

    1. $PT_CONFIG                       (explicit path, used by the tests)
    2. ./config.local.yaml              (private, gitignored — real values)
    3. <repo>/config.example.yaml       (public — every value is XXXX)

In the public showcase only (3) exists. Its values are the literal string
"XXXX"; they load as `Redacted` sentinels so that every module still imports
and the architecture can be read and navigated, but any attempt to *use* a
redacted value in arithmetic or a comparison raises RedactedParameterError
with the parameter's name.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

PLACEHOLDER = "XXXX"
_REPO_ROOT = Path(__file__).resolve().parents[2]


class RedactedParameterError(RuntimeError):
    """Raised when code tries to use a value that is redacted in the showcase."""


class Redacted:
    """Stand-in for a tuned value that is not published."""

    __slots__ = ("key",)

    def __init__(self, key: str):
        self.key = key

    def _fail(self, *_a, **_k):
        raise RedactedParameterError(
            f"Parameter '{self.key}' is redacted (tuned privately). "
            f"Provide it in config.local.yaml or via $PT_CONFIG."
        )

    # Any numeric use fails loudly; printing shows the placeholder.
    __float__ = __int__ = __index__ = __bool__ = _fail
    __lt__ = __le__ = __gt__ = __ge__ = _fail
    __add__ = __radd__ = __sub__ = __rsub__ = _fail
    __mul__ = __rmul__ = __truediv__ = __rtruediv__ = _fail
    __floordiv__ = __rfloordiv__ = __mod__ = __pow__ = _fail
    __neg__ = __pos__ = __abs__ = __round__ = _fail

    def __repr__(self) -> str:
        return PLACEHOLDER

    def __format__(self, _spec: str) -> str:
        return PLACEHOLDER


def _resolve_path() -> Path:
    env = os.environ.get("PT_CONFIG")
    if env:
        return Path(env)
    local = Path.cwd() / "config.local.yaml"
    if local.exists():
        return local
    return _REPO_ROOT / "config.example.yaml"


def _wrap(value: Any, key: str) -> Any:
    if isinstance(value, str) and value.strip() == PLACEHOLDER:
        return Redacted(key)
    if isinstance(value, list):
        return [_wrap(v, f"{key}[{i}]") for i, v in enumerate(value)]
    if isinstance(value, dict):
        return {k: _wrap(v, f"{key}.{k}" if key else k) for k, v in value.items()}
    return value


_CACHE: Optional[Dict[str, Any]] = None


def _load() -> Dict[str, Any]:
    global _CACHE
    if _CACHE is None:
        with open(_resolve_path(), "r", encoding="utf-8") as fh:
            _CACHE = _wrap(yaml.safe_load(fh) or {}, "")
    return _CACHE


def reload() -> None:
    """Drop the cache (tests switch configs between modules)."""
    global _CACHE
    _CACHE = None


def param(dotted_key: str) -> Any:
    """Return the value at `section.sub.key`, or a Redacted sentinel."""
    node: Any = _load()
    for part in dotted_key.split("."):
        if not isinstance(node, dict) or part not in node:
            raise KeyError(f"Missing config key '{dotted_key}' in {_resolve_path()}")
        node = node[part]
    return node
