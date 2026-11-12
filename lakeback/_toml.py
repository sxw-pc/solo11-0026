"""Compatibility import for TOML parsing on Python 3.10 and 3.11+."""

try:  # Python 3.11+
    import tomllib as _toml
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 fallback
    import tomli as _toml

TOMLDecodeError = _toml.TOMLDecodeError
load = _toml.load


def loads(value: str) -> dict:
    return _toml.loads(value)
