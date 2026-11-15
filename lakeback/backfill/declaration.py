"""Parse expected-change declaration documents into normalised rules.

Separated from :mod:`lakeback.backfill.expectations` so the parsing surface
stays small and the lookup/matching model stays independent of file formats.
"""

from __future__ import annotations

import json
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

from lakeback._toml import TOMLDecodeError, loads as loads_toml
from lakeback._util import NAME_RE
from lakeback.errors import ConfigError

DIRECTION_ALIASES = {
    "any": "any",
    "change": "any",
    "changed": "any",
    "*": "any",
    "increase": "increase",
    "up": "increase",
    "inc": "increase",
    "decrease": "decrease",
    "down": "decrease",
    "dec": "decrease",
}
FIELD_KEYS = ("field", "fields", "columns")
ROOT_LIST_KEYS = ("changes", "expectations", "expected_changes", "declarations")
ROOT_META_KEYS = {"format", "version"}
WRAPPER_KEYS = ("changes", "entries", "expectations")


def error(message: str, path: Path | None, *, details: dict | None = None) -> ConfigError:
    location = f" in {path}" if path else ""
    return ConfigError(f"{message}{location}", details=details)


def key_marker(value: Any) -> tuple:
    """Type-aware lookup marker so int and float spellings match."""
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, float)):
        try:
            return ("num", str(Decimal(str(value)).normalize()))
        except (InvalidOperation, ValueError):
            return ("num", str(value))
    return ("str", value)


def key_kind(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "str"


def _direction(value: Any, default: str, path: Path | None) -> str:
    if value is None:
        return default
    if not isinstance(value, str):
        raise error("expected-change direction must be a string", path)
    direction = DIRECTION_ALIASES.get(value.strip().lower())
    if direction is None:
        raise error(
            f"unsupported expected-change direction {value!r}",
            path,
            details={"allowed": ["any", "increase", "decrease"]},
        )
    return direction


def _field_name(value: Any, path: Path | None) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise error(f"invalid expected-change field name: {value!r}", path)
    return value


def _scalar_key(value: Any) -> bool:
    return isinstance(value, (str, int, float, bool)) and not isinstance(value, complex)


def _field_rules(fields: Any, default: str, path: Path | None) -> list[tuple[str, str]]:
    """Expand ``field``/``fields``/``columns`` syntax into ``(field, direction)``."""
    if isinstance(fields, str):
        return [(_field_name(fields, path), default)]
    if isinstance(fields, list):
        expanded: list[tuple[str, str]] = []
        for item in fields:
            if isinstance(item, str):
                expanded.append((_field_name(item, path), default))
            elif isinstance(item, dict):
                name = item.get("name", item.get("field"))
                expanded.append((
                    _field_name(name, path),
                    _direction(item.get("direction"), default, path),
                ))
            else:
                raise error("fields entries must be strings or tables", path)
        return expanded
    if isinstance(fields, dict):
        expanded = []
        for name, direction in fields.items():
            if isinstance(direction, dict):
                direction = direction.get("direction", default)
            expanded.append((_field_name(name, path), _direction(direction, default, path)))
        return expanded
    raise error("fields must be a string, list or table", path)


def _parse_entry(entry: Any, table_hint: str | None, path: Path | None) -> list[dict]:
    if not isinstance(entry, dict):
        raise error("each expected-change entry must be a table/object", path)
    table = entry.get("table", table_hint)
    if "table" in entry and table_hint is not None and entry["table"] != table_hint:
        raise error(
            f"entry table {entry['table']!r} conflicts with section table {table_hint!r}",
            path,
        )
    if not isinstance(table, str) or not NAME_RE.fullmatch(table):
        raise error(f"invalid table in expected-change entry: {table!r}", path)
    key_value = None
    for name in ("key", "primary_key", "key_value"):
        if name in entry:
            key_value = entry[name]
            break
    if not _scalar_key(key_value):
        raise error(
            f"expected-change entry for {table!r} needs a scalar key", path,
            details={"accepted": ["string", "integer", "float", "boolean"]},
        )
    default = _direction(entry.get("direction"), "any", path)
    specs: list[tuple[str, str]] = []
    for name in FIELD_KEYS:
        if name in entry:
            specs.extend(_field_rules(entry[name], default, path))
    if not specs:
        raise error(
            f"expected-change entry for {table!r}/{key_value!r} declares no fields",
            path,
        )
    return [
        {"table": table, "key": key_value, "field": field, "direction": direction}
        for field, direction in specs
    ]


def _parse_entries(spec: Any, table_hint: str | None, path: Path | None) -> list[dict]:
    if isinstance(spec, list):
        rules: list[dict] = []
        for item in spec:
            rules.extend(_parse_entry(item, table_hint, path))
        return rules
    if isinstance(spec, dict):
        for wrapper in WRAPPER_KEYS:
            if wrapper in spec:
                return _parse_entries(spec[wrapper], table_hint, path)
        return _parse_entry(spec, table_hint, path)
    raise error("expected-change declarations must be tables or lists", path)


def parse_document(document: Any, path: Path | None = None) -> list[dict]:
    """Accept the supported declaration shapes and return normalised rules."""
    if isinstance(document, list):
        return _parse_entries(document, None, path)
    if not isinstance(document, dict):
        raise error("declaration document must be a table/object or a list", path)
    if "table" in document:
        # A bare single entry, convenient for one-off JSON declarations.
        return _parse_entry(document, None, path)
    rules: list[dict] = []
    found = False
    for key in ROOT_LIST_KEYS:
        if key in document:
            rules.extend(_parse_entries(document[key], None, path))
            found = True
    if "tables" in document:
        tables = document["tables"]
        if not isinstance(tables, dict):
            raise error("[tables] must map table names to entries", path)
        for table, spec in tables.items():
            rules.extend(_parse_entries(spec, table, path))
        found = True
    payload = {
        key: value for key, value in document.items()
        if key not in ROOT_META_KEYS and key not in ROOT_LIST_KEYS and key != "tables"
    }
    if not found:
        # Convenience form: a mapping of table name -> list of entries.
        for table, spec in payload.items():
            if not isinstance(table, str) or not NAME_RE.fullmatch(table):
                raise error(f"unrecognised declaration key: {table!r}", path)
            rules.extend(_parse_entries(spec, table, path))
    elif payload:
        raise error(
            "declaration mixes [changes]/[tables] with unknown keys", path,
            details={"unexpected": sorted(payload)},
        )
    return rules


def dedupe(rules: Iterable[dict], path: Path | None = None) -> list[dict]:
    """Collapse duplicate rules and reject contradictory directions."""
    unique: dict[tuple, dict] = {}
    for rule in rules:
        marker = (rule["table"], key_marker(rule["key"]), rule["field"])
        previous = unique.get(marker)
        if previous is not None and previous["direction"] != rule["direction"]:
            raise error(
                "conflicting expected-change directions for "
                f"{rule['table']}.{rule['key']!r}.{rule['field']}",
                path,
                details={"first": previous["direction"], "second": rule["direction"]},
            )
        unique[marker] = rule
    return sorted(unique.values(), key=lambda item: (
        item["table"], key_marker(item["key"])[1], item["field"],
    ))


def _decode(text: str, path: Path) -> Any:
    if path.suffix.lower() == ".json":
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise error(f"cannot parse expected-change JSON: {exc}", path) from exc
    try:
        return loads_toml(text)
    except TOMLDecodeError as toml_exc:
        stripped = text.lstrip()
        if path.suffix.lower() not in (".toml", ".tml") or stripped[:1] in ("{", "["):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                pass
        raise error(f"cannot parse expected-change TOML: {toml_exc}", path) from toml_exc


def parse_file(path: Path) -> list[dict]:
    """Read and parse a TOML or JSON declaration file."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read expected-change declaration {path}: {exc}") from exc
    return dedupe(parse_document(_decode(text, path), path), path)
