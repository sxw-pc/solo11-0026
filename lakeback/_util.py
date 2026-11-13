"""Small shared helpers: canonical JSON, hashes, clocks and safe names."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

NAME_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}$")
REPORT_RE = re.compile(r"^rpt-[a-f0-9]{16}$")


def canonical_json(value: Any) -> str:
    """Return the one canonical JSON spelling used for all content hashes."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


MIN_UTC = datetime.min.replace(tzinfo=timezone.utc)
MAX_UTC = datetime.max.replace(tzinfo=timezone.utc)


def parse_timestamp(value: str | datetime) -> datetime:
    """Parse an ISO-8601 instant and normalise it to aware UTC.

    Naive timestamps are interpreted as UTC.  An instant whose UTC spelling
    would fall outside the representable calendar (for example
    ``0001-01-01T00:00:00+14:00``) is saturated to the nearest representable
    bound instead of leaking ``OverflowError``.  Such a value genuinely means
    "before the earliest snapshot" or "after the latest", so time travel then
    follows its normal boundary rules and keeps a stable exit code.
    """
    if isinstance(value, datetime):
        parsed = value
    else:
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"invalid ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    try:
        normalised = parsed.astimezone(timezone.utc)
    except (OverflowError, OSError, ValueError):
        # ``astimezone`` cannot represent the instant; decide the direction
        # from the unconverted wall-clock value and saturate.
        if parsed.year <= 1 and parsed.utcoffset() and parsed.utcoffset().total_seconds() > 0:
            return MIN_UTC
        if parsed.year >= 9999 and parsed.utcoffset() and parsed.utcoffset().total_seconds() < 0:
            return MAX_UTC
        raise ValueError(f"timestamp is outside the supported range: {value!r}")
    if normalised < MIN_UTC:
        return MIN_UTC
    if normalised > MAX_UTC:
        return MAX_UTC
    return normalised


def to_iso(moment: datetime) -> str:
    """Stable second-resolution UTC spelling used in manifests."""
    return parse_timestamp(moment).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalise_iso(value: str | datetime) -> str:
    """Canonical second-resolution UTC spelling for any accepted instant.

    ``created_at`` is always stored through this helper so that the manifest
    text is unambiguous, second-aligned and directly comparable: an offset
    string, a naive string and a ``Z`` string that denote the same instant all
    collapse to the same spelling.
    """
    return to_iso(parse_timestamp(value))


def second_floor(moment: datetime) -> datetime:
    """Truncate an aware datetime to whole seconds for ledger comparisons."""
    return moment.astimezone(timezone.utc).replace(microsecond=0)


def utc_now_iso() -> str:
    return to_iso(datetime.now(timezone.utc))


def json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    return "str"


def infer_type(values: Iterable[Any]) -> str:
    saw = None
    for value in values:
        if value is None or value == "":
            continue
        current = json_type(value)
        if current == "null":
            continue
        if saw is None:
            saw = current
        elif saw != current:
            if {saw, current} <= {"int", "float"}:
                saw = "float"
            else:
                return "str"
    return saw or "str"


def primary_key_sort(value: Any) -> tuple:
    """Comparable key that preserves numeric ordering for int primary keys."""
    if isinstance(value, bool):
        return (2, int(value))
    if isinstance(value, (int, float)):
        return (0, value)
    if isinstance(value, str):
        return (1, value)
    if value is None:
        return (3, "")
    return (4, canonical_json(value))


def row_sort_key(row: Mapping[str, Any], key: str) -> tuple:
    return primary_key_sort(row.get(key))


def stable_rows(rows: Iterable[Mapping[str, Any]], key: str | None = None) -> list[dict]:
    materialised = [dict(row) for row in rows]
    if key and all(key in row for row in materialised):
        materialised.sort(key=lambda row: primary_key_sort(row[key]))
    else:
        materialised.sort(key=canonical_json)
    return materialised


def partition_tuple(partition: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(sorted((str(name), str(value)) for name, value in partition.items()))


def partition_filters(values: Iterable[str] | None) -> dict[str, str]:
    """Parse CLI partition filters such as ``dt=2024-01-01``."""
    result: dict[str, str] = {}
    for raw in values or []:
        name, separator, value = raw.partition("=")
        if not separator or not name or not value or name in result:
            raise ValueError(f"invalid partition filter: {raw!r} (expected name=value)")
        if not NAME_RE.fullmatch(name):
            raise ValueError(f"invalid partition column: {name!r}")
        result[name] = value
    return result


def safe_child(base: Path, relative: str) -> Path:
    """Resolve a relative path while refusing traversal outside ``base``."""
    candidate = (base / relative).resolve()
    root = base.resolve()
    if candidate == root or root not in candidate.parents:
        raise ValueError(f"path escapes base directory: {relative!r}")
    return candidate


def tree_digest(root: Path) -> str:
    """Digest relative paths and bytes, independent of filesystem order."""
    digest = hashlib.sha256()
    for path in sorted((p for p in root.rglob("*") if p.is_file()), key=lambda p: p.as_posix()):
        relative = path.relative_to(root).as_posix()
        digest.update(relative.encode("utf-8") + b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def ensure_unique(rows: Iterable[Mapping[str, Any]], key: str) -> None:
    seen: set[Any] = set()
    for row in rows:
        value = row.get(key)
        if value is None:
            raise ValueError(f"row is missing primary key {key!r}")
        marker = canonical_json(value)
        if marker in seen:
            raise ValueError(f"duplicate primary key {key}={value!r}")
        seen.add(marker)
