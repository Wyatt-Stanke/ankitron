"""
Verification — cross-source field consistency checking.

Verification compares the value of a field across two (or more) sources
to detect data inconsistencies before export.
"""

from __future__ import annotations

import enum
import re
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any


class VerifyStrategy(enum.Enum):
    """How to compare two field values."""

    EXACT = "exact"
    CASE_INSENSITIVE = "case_insensitive"
    CONTAINS = "contains"
    NUMERIC_EXACT = "numeric_exact"
    NUMERIC_TOLERANCE = "numeric_tolerance"
    FUZZY_THRESHOLD = "fuzzy_threshold"


class OnMismatch(enum.Enum):
    """Behaviour when verification fails."""

    WARN = "warn"
    ERROR = "error"
    FLAG = "flag"
    SKIP = "skip"


class VerifyStatus(enum.Enum):
    MATCH = "match"
    MISMATCH = "mismatch"
    MISSING = "missing"


@dataclass
class VerificationCheck:
    """Result of a single row's verification."""

    pk: str
    field_name: str
    status: VerifyStatus
    primary_value: Any
    comparison_value: Any
    details: str = ""


@dataclass
class VerificationResult:
    """Aggregate result of verifying a field."""

    field_name: str
    strategy: VerifyStrategy
    on_mismatch: OnMismatch
    checks: list[VerificationCheck] = dc_field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.status == VerifyStatus.MATCH for c in self.checks)

    @property
    def mismatches(self) -> list[VerificationCheck]:
        return [c for c in self.checks if c.status == VerifyStatus.MISMATCH]

    @property
    def missing(self) -> list[VerificationCheck]:
        return [c for c in self.checks if c.status == VerifyStatus.MISSING]


@dataclass
class VerifyConfig:
    """Configuration for how a field should be verified."""

    strategy: VerifyStrategy = VerifyStrategy.EXACT
    on_mismatch: OnMismatch = OnMismatch.WARN
    tolerance: float = 0.0
    threshold: float = 0.8

    def check(self, primary: Any, comparison: Any, pk: str, field_name: str) -> VerificationCheck:
        """Check two values and return a VerificationCheck."""
        if primary is None and comparison is None:
            return VerificationCheck(
                pk=pk,
                field_name=field_name,
                status=VerifyStatus.MATCH,
                primary_value=primary,
                comparison_value=comparison,
            )
        if primary is None or comparison is None:
            return VerificationCheck(
                pk=pk,
                field_name=field_name,
                status=VerifyStatus.MISSING,
                primary_value=primary,
                comparison_value=comparison,
                details="one side is None",
            )
        match = _compare(primary, comparison, self.strategy, self.tolerance, self.threshold)
        status = VerifyStatus.MATCH if match else VerifyStatus.MISMATCH
        return VerificationCheck(
            pk=pk,
            field_name=field_name,
            status=status,
            primary_value=primary,
            comparison_value=comparison,
            details=f"strategy={self.strategy.value}",
        )


def _compare(a: Any, b: Any, strategy: VerifyStrategy, tolerance: float, threshold: float) -> bool:
    """Compare two values using the given strategy."""
    if strategy == VerifyStrategy.EXACT:
        return _norm(a) == _norm(b)

    if strategy == VerifyStrategy.CASE_INSENSITIVE:
        return _norm(a).lower() == _norm(b).lower()

    if strategy == VerifyStrategy.CONTAINS:
        sa, sb = _norm(a).lower(), _norm(b).lower()
        return sa in sb or sb in sa

    if strategy == VerifyStrategy.NUMERIC_EXACT:
        try:
            return float(_numeric_clean(a)) == float(_numeric_clean(b))
        except (ValueError, TypeError):
            return False

    if strategy == VerifyStrategy.NUMERIC_TOLERANCE:
        try:
            fa, fb = float(_numeric_clean(a)), float(_numeric_clean(b))
        except (ValueError, TypeError):
            return False
        if fb == 0:
            return fa == 0
        return abs(fa - fb) / abs(fb) <= tolerance

    if strategy == VerifyStrategy.FUZZY_THRESHOLD:
        return _fuzzy_ratio(_norm(a), _norm(b)) >= threshold

    return False


def _norm(val: Any) -> str:
    """Normalize a value to a stripped string."""
    return str(val).strip()


def _numeric_clean(val: Any) -> str:
    """Strip non-numeric characters except digits, dots, minus."""
    s = str(val).strip()
    return re.sub(r"[^\d.\-]", "", s)


def _fuzzy_ratio(a: str, b: str) -> float:
    """Simple character-level similarity ratio (no external deps)."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    a_lower, b_lower = a.lower(), b.lower()
    if a_lower == b_lower:
        return 1.0
    # Simple longest common subsequence ratio
    m, n = len(a_lower), len(b_lower)
    if m > 500 or n > 500:
        # Fall back to prefix match for very long strings
        common = 0
        for ca, cb in zip(a_lower, b_lower, strict=False):
            if ca == cb:
                common += 1
            else:
                break
        return (2 * common) / (m + n)
    # LCS length via DP (O(m*n))
    prev = [0] * (n + 1)
    for i in range(1, m + 1):
        curr = [0] * (n + 1)
        for j in range(1, n + 1):
            if a_lower[i - 1] == b_lower[j - 1]:
                curr[j] = prev[j - 1] + 1
            else:
                curr[j] = max(prev[j], curr[j - 1])
        prev = curr
    lcs = prev[n]
    return (2 * lcs) / (m + n)


def run_verification(
    field_name: str,
    config: VerifyConfig,
    primary_rows: list[dict[str, Any]],
    comparison_rows: list[dict[str, Any]],
    pk_field: str,
) -> VerificationResult:
    """Run verification for a specific field across two source datasets.

    Both datasets must be keyed by the same PK field.
    """
    comp_by_pk = {str(row.get(pk_field)): row for row in comparison_rows}
    result = VerificationResult(
        field_name=field_name,
        strategy=config.strategy,
        on_mismatch=config.on_mismatch,
    )
    for row in primary_rows:
        pk = str(row.get(pk_field, "?"))
        primary_val = row.get(field_name)
        comp_row = comp_by_pk.get(pk)
        if comp_row is None:
            result.checks.append(
                VerificationCheck(
                    pk=pk,
                    field_name=field_name,
                    status=VerifyStatus.MISSING,
                    primary_value=primary_val,
                    comparison_value=None,
                    details="no match in comparison source",
                )
            )
        else:
            comp_val = comp_row.get(field_name)
            result.checks.append(config.check(primary_val, comp_val, pk, field_name))
    return result
