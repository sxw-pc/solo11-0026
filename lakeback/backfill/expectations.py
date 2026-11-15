"""Lookup and matching model for expected-change declarations.

A declaration names a table, a primary-key value and one or more fields that
are allowed to change, together with the allowed direction of that change.
The same declaration is consulted both for row-level differences and for the
per-field checksums inside each partition, so an undeclared field in a
partially-exempted partition is still reported as residual.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable, Mapping

from lakeback._util import canonical_json, sha256_text
from lakeback.backfill.declaration import (
    dedupe,
    key_kind,
    key_marker,
    parse_document,
    parse_file,
)
from lakeback.errors import ConfigError

DECLARATION_FORMAT = 1


def _compatible_kind(declared: str, actual: str) -> bool:
    if actual == "null":
        return True
    if declared in {"int", "float"}:
        return actual in {"int", "float"}
    return declared == actual


class ExpectedChanges:
    """Normalised, lookup-friendly declaration of allowed field changes."""

    def __init__(self, rules: Iterable[dict], *, source: Path | None = None) -> None:
        self.rules = tuple(rules)
        self.source = Path(source).resolve() if source else None
        self.digest = sha256_text(canonical_json(self.fingerprint_payload()))
        self._lookup: dict[str, dict[tuple[tuple, str], dict]] = {}
        for rule in self.rules:
            table_rules = self._lookup.setdefault(rule["table"], {})
            table_rules[(key_marker(rule["key"]), rule["field"])] = rule

    def for_table(self, table: str) -> tuple[dict, ...]:
        return tuple(rule for rule in self.rules if rule["table"] == table)

    def validate_table(
        self,
        table: str,
        before_schema: Mapping[str, str],
        after_schema: Mapping[str, str],
        primary_key: str | None = None,
    ) -> None:
        """Reject declarations that can never match the audited table."""
        available = set(before_schema) | set(after_schema)
        key_kinds = {
            schema[primary_key] for schema in (before_schema, after_schema)
            if primary_key and schema.get(primary_key) is not None
        }
        for rule in self.for_table(table):
            field = rule["field"]
            if field not in available:
                raise ConfigError(
                    f"expected-change field {field!r} is not in table {table!r}",
                    details={"table": table, "field": field,
                             "fields": sorted(available)},
                )
            if key_kinds:
                declared = key_kind(rule["key"])
                if not any(_compatible_kind(declared, kind) for kind in key_kinds):
                    raise ConfigError(
                        f"expected-change key {rule['key']!r} is a {declared} value "
                        f"but primary key {primary_key!r} has type "
                        f"{'/'.join(sorted(key_kinds))}",
                        details={"table": table, "key": rule["key"],
                                 "declared_type": declared,
                                 "key_types": sorted(key_kinds),
                                 "hint": "quote string keys; write numeric keys "
                                         "without quotes"},
                    )
            if rule["direction"] not in {"increase", "decrease"}:
                continue
            types = {
                name: mapping.get(field) for name, mapping in (
                    ("before", before_schema), ("after", after_schema)
                ) if mapping.get(field) is not None
            }
            # Raising is only unambiguous when no side is numeric; a mixed
            # before/after type change is a structural issue the audit reports
            # through its normal checks.
            if all(kind not in {"int", "float"} for kind in types.values()):
                raise ConfigError(
                    f"expected-change direction {rule['direction']!r} needs a "
                    f"numeric field, but {table}.{field} has types {types}",
                    details={"table": table, "field": field,
                             "direction": rule["direction"], "types": types},
                )

    def matches(
        self, table: str, key_value: Any, field: str, before: Any, after: Any
    ) -> bool:
        """Whether one concrete field change is covered by the declaration."""
        if before == after:
            return False
        rule = self._lookup.get(table, {}).get((key_marker(key_value), field))
        if rule is None:
            return False
        direction = rule["direction"]
        if direction == "any":
            return True
        if isinstance(before, bool) or isinstance(after, bool):
            return False
        if not isinstance(before, (int, float)) or not isinstance(after, (int, float)):
            return False
        return after > before if direction == "increase" else after < before

    def split_modified_rows(
        self, table: str, modified: Iterable[dict]
    ) -> tuple[list[dict], list[dict]]:
        """Split row-level changes into expected and residual field lists."""
        expected: list[dict] = []
        residual: list[dict] = []
        for item in modified:
            expected_fields: list[dict] = []
            residual_fields: list[dict] = []
            for change in item["fields"]:
                target = (
                    expected_fields
                    if self.matches(
                        table, item["key"], change["name"],
                        change["before"], change["after"],
                    )
                    else residual_fields
                )
                target.append(change)
            if expected_fields:
                expected.append({"key": item["key"], "fields": expected_fields})
            if residual_fields:
                residual.append({"key": item["key"], "fields": residual_fields})
        return expected, residual

    def explain_partition_fields(
        self,
        table: str,
        before_rows: list[dict],
        after_rows: list[dict],
        key: str,
        candidate_fields: Iterable[str],
    ) -> tuple[list[str], list[str]]:
        """Split partition field mismatches into fully-expected and residual.

        A field is expected only when the partition has the same key set on
        both sides and *every* value change for that field is declared with
        the right direction. A single undeclared change keeps the field
        residual, even if another field in the same partition is declared.
        """
        candidates = sorted(set(candidate_fields))
        if not candidates:
            return [], []
        left = {key_marker(row.get(key)): row for row in before_rows}
        right = {key_marker(row.get(key)): row for row in after_rows}
        if (
            len(left) != len(before_rows)
            or len(right) != len(after_rows)
            or set(left) != set(right)
        ):
            # Row membership changes are never excused by a field declaration.
            return [], candidates
        expected: list[str] = []
        residual: list[str] = []
        for field in candidates:
            changed = False
            allowed = True
            for marker in left:
                before_value = left[marker].get(field)
                after_value = right[marker].get(field)
                if before_value == after_value:
                    continue
                changed = True
                if not self.matches(
                    table, left[marker][key], field, before_value, after_value
                ):
                    allowed = False
                    break
            (expected if changed and allowed else residual).append(field)
        return expected, residual

    def fingerprint_payload(self) -> dict:
        """Canonical declaration content included in the report fingerprint.

        Numeric key spellings are normalised so ``key = 2`` and ``key = 2.0``
        select the same rows and produce the same fingerprint.
        """
        return {
            "format": DECLARATION_FORMAT,
            "rules": [
                {
                    "table": rule["table"],
                    "key": list(key_marker(rule["key"])),
                    "field": rule["field"],
                    "direction": rule["direction"],
                }
                for rule in self.rules
            ],
        }

    def metadata(self, table: str) -> dict:
        """Stable description of the declaration used for one audit table."""
        return {
            "declared": True,
            "path": str(self.source) if self.source else None,
            "digest": self.digest,
            "format": DECLARATION_FORMAT,
            "rules": len(self.rules),
            "table_rules": len(self.for_table(table)),
        }


def load_expected_changes(source: Any) -> ExpectedChanges | None:
    """Load a declaration from a path, or normalise an already-parsed value."""
    if source is None:
        return None
    if isinstance(source, ExpectedChanges):
        return source
    if isinstance(source, (dict, list)):
        return ExpectedChanges(dedupe(parse_document(source)))
    path = Path(source).expanduser()
    if not path.is_file():
        raise ConfigError(
            f"expected-change declaration not found: {path}",
            details={"hint": "pass a TOML or JSON declaration file"},
        )
    return ExpectedChanges(parse_file(path), source=path)
