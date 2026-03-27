"""
Validation — deck-level assertions about data quality.

Validators check properties of the entire dataset after all sources have been
fetched and transforms applied. They run before export and produce warnings or
errors.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import TYPE_CHECKING, Any

from ankitron.enums import Severity

if TYPE_CHECKING:
    from collections.abc import Callable

    from ankitron.deck import Field


@dataclass
class ValidatorResult:
    """Result of running a single validator."""

    name: str
    passed: bool
    severity: Severity
    messages: list[str] = dc_field(default_factory=list)


class _Validator:
    """Base for all validator instances."""

    def __init__(self, name: str, severity: Severity) -> None:
        self._name = name
        self._severity = severity

    def run(
        self, rows: list[dict[str, Any]], field_attrs: dict[str, Any] | None = None
    ) -> ValidatorResult:
        raise NotImplementedError

    @property
    def name(self) -> str:
        return self._name

    @property
    def severity(self) -> Severity:
        return self._severity


class _RowCountValidator(_Validator):
    def __init__(self, expected: int, tolerance: int, severity: Severity) -> None:
        super().__init__("row_count", severity)
        self._expected = expected
        self._tolerance = tolerance

    def run(
        self, rows: list[dict[str, Any]], field_attrs: dict[str, Any] | None = None
    ) -> ValidatorResult:
        actual = len(rows)
        lo = self._expected - self._tolerance
        hi = self._expected + self._tolerance
        if lo <= actual <= hi:
            return ValidatorResult(
                name=self._name,
                passed=True,
                severity=self._severity,
                messages=[f"{actual} rows (expected {self._expected})"],
            )
        return ValidatorResult(
            name=self._name,
            passed=False,
            severity=self._severity,
            messages=[f"Expected {self._expected}±{self._tolerance} rows, got {actual}"],
        )


class _FieldTypeValidator(_Validator):
    def __init__(self, field: Field, expected_type: type, severity: Severity) -> None:
        super().__init__(f"field_type({field.name})", severity)
        self._field = field
        self._expected_type = expected_type

    def run(
        self, rows: list[dict[str, Any]], field_attrs: dict[str, Any] | None = None
    ) -> ValidatorResult:
        attr = self._field.name
        bad = []
        for row in rows:
            val = row.get(attr)
            if val is not None and not isinstance(val, self._expected_type):
                bad.append(f"{row.get('_pk', '?')}: {type(val).__name__}")
        if not bad:
            return ValidatorResult(
                name=self._name,
                passed=True,
                severity=self._severity,
                messages=[f"all {self._expected_type.__name__}"],
            )
        return ValidatorResult(
            name=self._name, passed=False, severity=self._severity, messages=bad[:10]
        )


class _FieldRangeValidator(_Validator):
    def __init__(
        self, field: Field, min_val: float | None, max_val: float | None, severity: Severity
    ) -> None:
        super().__init__(f"field_range({field.name})", severity)
        self._field = field
        self._min = min_val
        self._max = max_val

    def run(
        self, rows: list[dict[str, Any]], field_attrs: dict[str, Any] | None = None
    ) -> ValidatorResult:
        attr = self._field.name
        bad = []
        for row in rows:
            val = row.get(attr)
            if val is None:
                continue
            try:
                v = float(val)
            except (ValueError, TypeError):
                continue
            if self._min is not None and v < self._min:
                bad.append(f"{row.get('_pk', '?')}: {v} < {self._min}")
            if self._max is not None and v > self._max:
                bad.append(f"{row.get('_pk', '?')}: {v} > {self._max}")
        if not bad:
            parts = []
            if self._min is not None:
                parts.append(f"{self._min:,}")
            if self._max is not None:
                parts.append(f"{self._max:,}")
            return ValidatorResult(
                name=self._name,
                passed=True,
                severity=self._severity,
                messages=[f"all within [{' — '.join(parts)}]"],
            )
        return ValidatorResult(
            name=self._name, passed=False, severity=self._severity, messages=bad[:10]
        )


class _FieldValuesValidator(_Validator):
    def __init__(self, field: Field, allowed: list[Any], severity: Severity) -> None:
        super().__init__(f"field_values({field.name})", severity)
        self._field = field
        self._allowed = set(allowed)

    def run(
        self, rows: list[dict[str, Any]], field_attrs: dict[str, Any] | None = None
    ) -> ValidatorResult:
        attr = self._field.name
        bad = []
        for row in rows:
            val = row.get(attr)
            if val is not None and val not in self._allowed:
                pk = row.get("_pk", "?")
                bad.append(f"{pk} → {val!r}")
        if not bad:
            return ValidatorResult(
                name=self._name, passed=True, severity=self._severity, messages=["all valid"]
            )
        return ValidatorResult(
            name=self._name, passed=False, severity=self._severity, messages=bad[:10]
        )


class _UniqueValidator(_Validator):
    def __init__(self, field: Field, severity: Severity) -> None:
        super().__init__(f"unique({field.name})", severity)
        self._field = field

    def run(
        self, rows: list[dict[str, Any]], field_attrs: dict[str, Any] | None = None
    ) -> ValidatorResult:
        attr = self._field.name
        seen: dict[Any, int] = {}
        for row in rows:
            v = row.get(attr)
            if v is not None:
                seen[v] = seen.get(v, 0) + 1
        dups = {k: cnt for k, cnt in seen.items() if cnt > 1}
        if not dups:
            return ValidatorResult(
                name=self._name, passed=True, severity=self._severity, messages=["all unique"]
            )
        msgs = [f"{k!r} ({cnt}x)" for k, cnt in list(dups.items())[:10]]
        return ValidatorResult(
            name=self._name, passed=False, severity=self._severity, messages=msgs
        )


class _NoDuplicatesValidator(_Validator):
    def __init__(self, fields: tuple[Field, ...], severity: Severity) -> None:
        names = ", ".join(f.name for f in fields)
        super().__init__(f"no_duplicates({names})", severity)
        self._fields = fields

    def run(
        self, rows: list[dict[str, Any]], field_attrs: dict[str, Any] | None = None
    ) -> ValidatorResult:
        attrs = [f.name for f in self._fields]
        seen: dict[tuple, int] = {}
        for row in rows:
            key = tuple(row.get(a) for a in attrs)
            seen[key] = seen.get(key, 0) + 1
        dups = {k: cnt for k, cnt in seen.items() if cnt > 1}
        if not dups:
            return ValidatorResult(
                name=self._name,
                passed=True,
                severity=self._severity,
                messages=["all unique combinations"],
            )
        msgs = [f"{k} ({cnt}x)" for k, cnt in list(dups.items())[:10]]
        return ValidatorResult(
            name=self._name, passed=False, severity=self._severity, messages=msgs
        )


class _CustomValidator(_Validator):
    def __init__(
        self, fn: Callable[[list[dict]], list[str]], name: str, severity: Severity
    ) -> None:
        super().__init__(name, severity)
        self._fn = fn

    def run(
        self, rows: list[dict[str, Any]], field_attrs: dict[str, Any] | None = None
    ) -> ValidatorResult:
        messages = self._fn(rows)
        return ValidatorResult(
            name=self._name,
            passed=len(messages) == 0,
            severity=self._severity,
            messages=messages,
        )


class Validate:
    """Factory class for creating validators."""

    @staticmethod
    def row_count(
        expected: int, tolerance: int = 0, severity: Severity = Severity.ERROR
    ) -> _Validator:
        return _RowCountValidator(expected, tolerance, severity)

    @staticmethod
    def field_type(
        field: Field, expected_type: type, severity: Severity = Severity.ERROR
    ) -> _Validator:
        return _FieldTypeValidator(field, expected_type, severity)

    @staticmethod
    def field_range(
        field: Field,
        *,
        min: float | None = None,
        max: float | None = None,
        severity: Severity = Severity.WARN,
    ) -> _Validator:
        return _FieldRangeValidator(field, min, max, severity)

    @staticmethod
    def field_values(
        field: Field, allowed: list[Any], severity: Severity = Severity.ERROR
    ) -> _Validator:
        return _FieldValuesValidator(field, allowed, severity)

    @staticmethod
    def unique(field: Field, severity: Severity = Severity.ERROR) -> _Validator:
        return _UniqueValidator(field, severity)

    @staticmethod
    def no_duplicates(*fields: Field, severity: Severity = Severity.ERROR) -> _Validator:
        return _NoDuplicatesValidator(fields, severity)

    @staticmethod
    def custom(
        fn: Callable[[list[dict]], list[str]],
        name: str = "custom",
        severity: Severity = Severity.WARN,
    ) -> _Validator:
        return _CustomValidator(fn, name, severity)


def run_validators(
    validators: list[_Validator], rows: list[dict[str, Any]]
) -> list[ValidatorResult]:
    """Run all validators against the dataset and return results."""
    return [v.run(rows) for v in validators]
