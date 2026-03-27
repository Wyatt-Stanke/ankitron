"""
Transform API — declarative, composable, introspectable value transformations.

Transforms convert raw source values into display-ready values for flashcards.
They operate in the pipeline between source data and formatting:

    Source → raw value → Transform → transformed value → fmt → display string
"""

from __future__ import annotations

import math
import re
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class Transform(ABC):
    """Base class for all transforms."""

    def apply(self, value: Any) -> Any:
        if value is None:
            return None
        return self._apply(value)

    @abstractmethod
    def _apply(self, value: Any) -> Any: ...

    def then(self, next_transform: Transform) -> Transform:
        return ChainedTransform(self, next_transform)

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def description(self) -> str: ...

    @property
    def is_dataset_aware(self) -> bool:
        return False

    def __repr__(self) -> str:
        return f"Transform({self.description})"

    # ── Static factory methods for built-in transforms ──

    @staticmethod
    def round_to_nearest(thresholds: dict[float, int]) -> Transform:
        return _RoundToNearest(thresholds)

    @staticmethod
    def round_to(*, decimals: int | None = None, sig_figs: int | None = None) -> Transform:
        return _RoundTo(decimals=decimals, sig_figs=sig_figs)

    @staticmethod
    def clamp(*, min: float | None = None, max: float | None = None) -> Transform:
        return _Clamp(min_val=min, max_val=max)

    @staticmethod
    def percentile_bucket(buckets: int = 10, label_range: tuple[int, int] = (1, 10)) -> Transform:
        return _PercentileBucket(buckets=buckets, label_range=label_range)

    @staticmethod
    def abbreviate(precision: int = 1) -> Transform:
        return _Abbreviate(precision=precision)

    @staticmethod
    def upper() -> Transform:
        return _Upper()

    @staticmethod
    def lower() -> Transform:
        return _Lower()

    @staticmethod
    def title() -> Transform:
        return _Title()

    @staticmethod
    def truncate(max_length: int, suffix: str = "...") -> Transform:
        return _Truncate(max_length=max_length, suffix=suffix)

    @staticmethod
    def replace(old: str, new: str) -> Transform:
        return _Replace(old=old, new=new)

    @staticmethod
    def regex_extract(pattern: str, group: int = 0) -> Transform:
        return _RegexExtract(pattern=pattern, group=group)

    @staticmethod
    def strip_html() -> Transform:
        return _StripHtml()

    @staticmethod
    def strip_wiki() -> Transform:
        return _StripWiki()

    @staticmethod
    def year_only() -> Transform:
        return _YearOnly()

    @staticmethod
    def decade() -> Transform:
        return _Decade()

    @staticmethod
    def relative_date() -> Transform:
        return _RelativeDate()

    @staticmethod
    def era(eras: dict[float, str]) -> Transform:
        return _Era(eras=eras)

    @staticmethod
    def map_values(mapping: dict[Any, Any], default: Any = None) -> Transform:
        return _MapValues(mapping=mapping, default=default)

    @staticmethod
    def bucket(thresholds: dict[float, str]) -> Transform:
        return _Bucket(thresholds=thresholds)

    @staticmethod
    def custom(
        fn: Callable[[Any], Any],
        name: str = "custom",
        description: str | None = None,
        none_safe: bool = False,
    ) -> Transform:
        return _Custom(fn=fn, custom_name=name, custom_description=description, none_safe=none_safe)


class ChainedTransform(Transform):
    """Two transforms applied in sequence."""

    def __init__(self, first: Transform, second: Transform) -> None:
        self.first = first
        self.second = second

    def _apply(self, value: Any) -> Any:
        intermediate = self.first.apply(value)
        return self.second.apply(intermediate)

    @property
    def name(self) -> str:
        return f"{self.first.name} → {self.second.name}"

    @property
    def description(self) -> str:
        return f"{self.first.description} → {self.second.description}"

    @property
    def is_dataset_aware(self) -> bool:
        return any(t.is_dataset_aware for t in self.steps)

    @property
    def steps(self) -> list[Transform]:
        result: list[Transform] = []
        for t in [self.first, self.second]:
            if isinstance(t, ChainedTransform):
                result.extend(t.steps)
            else:
                result.append(t)
        return result


class DatasetAwareTransform(Transform):
    """Base for transforms that need the full dataset."""

    @property
    def is_dataset_aware(self) -> bool:
        return True

    def _apply(self, value: Any) -> Any:
        raise RuntimeError(
            f"{self.name} is a dataset-aware transform and cannot be "
            f"applied to individual values. Use apply_batch()."
        )

    @abstractmethod
    def apply_batch(self, values: list[Any]) -> list[Any]: ...


# ── Numeric transforms ─────────────────────────────────────────


class _RoundToNearest(Transform):
    def __init__(self, thresholds: dict[float, int]) -> None:
        self._thresholds = dict(sorted(thresholds.items()))

    def _apply(self, value: Any) -> Any:
        v = float(value)
        for upper_bound, granularity in self._thresholds.items():
            if v < upper_bound:
                return int(round(v / granularity) * granularity)
        # Fallback to last granularity
        last_granularity = list(self._thresholds.values())[-1]
        return int(round(v / last_granularity) * last_granularity)

    @property
    def name(self) -> str:
        return "round_to_nearest"

    @property
    def description(self) -> str:
        parts = []
        for bound, gran in self._thresholds.items():
            b = "∞" if math.isinf(bound) else f"<{_abbrev(bound)}"
            parts.append(f"{b}: {_abbrev(gran)}")
        return f"round_to_nearest({{{', '.join(parts)}}})"


class _RoundTo(Transform):
    def __init__(self, *, decimals: int | None, sig_figs: int | None) -> None:
        if decimals is None and sig_figs is None:
            raise ValueError("Specify either decimals= or sig_figs=")
        self._decimals = decimals
        self._sig_figs = sig_figs

    def _apply(self, value: Any) -> Any:
        v = float(value)
        if self._decimals is not None:
            return round(v, self._decimals)
        n = self._sig_figs
        if v == 0:
            return 0
        magnitude = math.floor(math.log10(abs(v)))
        return round(v, -int(magnitude) + (n - 1))

    @property
    def name(self) -> str:
        return "round_to"

    @property
    def description(self) -> str:
        if self._decimals is not None:
            return f"round_to(decimals={self._decimals})"
        return f"round_to(sig_figs={self._sig_figs})"


class _Clamp(Transform):
    def __init__(self, *, min_val: float | None, max_val: float | None) -> None:
        self._min = min_val
        self._max = max_val

    def _apply(self, value: Any) -> Any:
        v = float(value)
        if self._min is not None:
            v = max(v, self._min)
        if self._max is not None:
            v = min(v, self._max)
        return v

    @property
    def name(self) -> str:
        return "clamp"

    @property
    def description(self) -> str:
        parts = []
        if self._min is not None:
            parts.append(f"min={self._min}")
        if self._max is not None:
            parts.append(f"max={self._max}")
        return f"clamp({', '.join(parts)})"


class _PercentileBucket(DatasetAwareTransform):
    def __init__(self, buckets: int, label_range: tuple[int, int]) -> None:
        self._buckets = buckets
        self._label_range = label_range

    def apply_batch(self, values: list[Any]) -> list[Any]:
        non_none = [(i, float(v)) for i, v in enumerate(values) if v is not None]
        if not non_none:
            return [None] * len(values)
        sorted_vals = sorted(non_none, key=lambda x: x[1])
        n = len(sorted_vals)
        lo, hi = self._label_range

        results: list[Any] = [None] * len(values)
        for rank, (idx, _) in enumerate(sorted_vals):
            percentile = rank / n
            bucket = int(percentile * self._buckets) + lo
            bucket = min(bucket, hi)
            results[idx] = bucket
        return results

    @property
    def name(self) -> str:
        return "percentile_bucket"

    @property
    def description(self) -> str:
        return f"percentile_bucket(buckets={self._buckets}, range={self._label_range})"


class _Abbreviate(Transform):
    def __init__(self, precision: int) -> None:
        self._precision = precision

    def _apply(self, value: Any) -> Any:
        v = float(value)
        abs_v = abs(v)
        if abs_v >= 1_000_000_000:
            return f"{v / 1_000_000_000:.{self._precision}f}B"
        if abs_v >= 1_000_000:
            return f"{v / 1_000_000:.{self._precision}f}M"
        if abs_v >= 1_000:
            return f"{v / 1_000:.{self._precision}f}K"
        return str(int(v)) if v == int(v) else str(v)

    @property
    def name(self) -> str:
        return "abbreviate"

    @property
    def description(self) -> str:
        return f"abbreviate(precision={self._precision})"


# ── String transforms ──────────────────────────────────────────


class _Upper(Transform):
    def _apply(self, value: Any) -> str:
        return str(value).upper()

    @property
    def name(self) -> str:
        return "upper"

    @property
    def description(self) -> str:
        return "upper()"


class _Lower(Transform):
    def _apply(self, value: Any) -> str:
        return str(value).lower()

    @property
    def name(self) -> str:
        return "lower"

    @property
    def description(self) -> str:
        return "lower()"


class _Title(Transform):
    def _apply(self, value: Any) -> str:
        return str(value).title()

    @property
    def name(self) -> str:
        return "title"

    @property
    def description(self) -> str:
        return "title()"


class _Truncate(Transform):
    def __init__(self, max_length: int, suffix: str) -> None:
        self._max_length = max_length
        self._suffix = suffix

    def _apply(self, value: Any) -> str:
        s = str(value)
        if len(s) <= self._max_length:
            return s
        return s[: self._max_length - len(self._suffix)] + self._suffix

    @property
    def name(self) -> str:
        return "truncate"

    @property
    def description(self) -> str:
        return f"truncate({self._max_length})"


class _Replace(Transform):
    def __init__(self, old: str, new: str) -> None:
        self._old = old
        self._new = new

    def _apply(self, value: Any) -> str:
        return str(value).replace(self._old, self._new)

    @property
    def name(self) -> str:
        return "replace"

    @property
    def description(self) -> str:
        return f"replace({self._old!r}, {self._new!r})"


class _RegexExtract(Transform):
    def __init__(self, pattern: str, group: int) -> None:
        self._pattern = re.compile(pattern)
        self._group = group

    def _apply(self, value: Any) -> Any:
        m = self._pattern.search(str(value))
        if m is None:
            return None
        return m.group(self._group)

    @property
    def name(self) -> str:
        return "regex_extract"

    @property
    def description(self) -> str:
        return f"regex_extract({self._pattern.pattern!r}, group={self._group})"


class _StripHtml(Transform):
    _TAG_RE = re.compile(r"<[^>]+>")

    def _apply(self, value: Any) -> str:
        return self._TAG_RE.sub("", str(value))

    @property
    def name(self) -> str:
        return "strip_html"

    @property
    def description(self) -> str:
        return "strip_html()"


class _StripWiki(Transform):
    _LINK_RE = re.compile(r"\[\[(?:[^|\]]*\|)?([^\]]*)\]\]")

    def _apply(self, value: Any) -> str:
        s = str(value)
        s = self._LINK_RE.sub(r"\1", s)
        s = re.sub(r"'{2,3}", "", s)  # bold/italic
        return s  # noqa: RET504

    @property
    def name(self) -> str:
        return "strip_wiki"

    @property
    def description(self) -> str:
        return "strip_wiki()"


# ── Date transforms ────────────────────────────────────────────


class _YearOnly(Transform):
    def _apply(self, value: Any) -> int:
        if isinstance(value, (datetime, date)):
            return value.year
        s = str(value)
        m = re.search(r"\d{4}", s)
        if m:
            return int(m.group())
        return int(float(s))

    @property
    def name(self) -> str:
        return "year_only"

    @property
    def description(self) -> str:
        return "year_only()"


class _Decade(Transform):
    def _apply(self, value: Any) -> str:
        year = value.year if isinstance(value, (datetime, date)) else int(float(str(value)))
        decade = (year // 10) * 10
        return f"{decade}s"

    @property
    def name(self) -> str:
        return "decade"

    @property
    def description(self) -> str:
        return "decade()"


class _RelativeDate(Transform):
    def _apply(self, value: Any) -> str:
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, date):
            dt = datetime(value.year, value.month, value.day, tzinfo=UTC)
        else:
            # Try parsing ISO date string
            s = str(value)
            try:
                dt = datetime.fromisoformat(s)
            except ValueError:
                # Try extracting just the year
                m = re.search(r"\d{4}", s)
                if m:
                    dt = datetime(int(m.group()), 1, 1, tzinfo=UTC)
                else:
                    return str(value)

        now = datetime.now(tz=UTC)
        delta = now - dt
        years = delta.days // 365
        if years == 0:
            months = delta.days // 30
            if months == 0:
                return f"{delta.days} days ago"
            return f"{months} month{'s' if months != 1 else ''} ago"
        return f"{years} year{'s' if years != 1 else ''} ago"

    @property
    def name(self) -> str:
        return "relative_date"

    @property
    def description(self) -> str:
        return "relative_date()"


class _Era(Transform):
    def __init__(self, eras: dict[float, str]) -> None:
        self._eras = dict(sorted(eras.items()))

    def _apply(self, value: Any) -> str:
        year = value.year if isinstance(value, (datetime, date)) else int(float(str(value)))
        for upper_bound, label in self._eras.items():
            if year < upper_bound:
                return label
        return list(self._eras.values())[-1]

    @property
    def name(self) -> str:
        return "era"

    @property
    def description(self) -> str:
        return f"era({len(self._eras)} periods)"


# ── Categorical transforms ─────────────────────────────────────


class _MapValues(Transform):
    def __init__(self, mapping: dict[Any, Any], default: Any) -> None:
        self._mapping = mapping
        self._default = default

    def _apply(self, value: Any) -> Any:
        return self._mapping.get(value, self._default)

    @property
    def name(self) -> str:
        return "map_values"

    @property
    def description(self) -> str:
        return f"map_values({len(self._mapping)} entries)"


class _Bucket(Transform):
    def __init__(self, thresholds: dict[float, str]) -> None:
        self._thresholds = dict(sorted(thresholds.items()))

    def _apply(self, value: Any) -> str:
        v = float(value)
        for upper_bound, label in self._thresholds.items():
            if v < upper_bound:
                return label
        return list(self._thresholds.values())[-1]

    @property
    def name(self) -> str:
        return "bucket"

    @property
    def description(self) -> str:
        return f"bucket({len(self._thresholds)} levels)"


# ── Custom escape hatch ────────────────────────────────────────


class _Custom(Transform):
    def __init__(
        self,
        fn: Callable[[Any], Any],
        custom_name: str,
        custom_description: str | None,
        none_safe: bool,
    ) -> None:
        self._fn = fn
        self._custom_name = custom_name
        self._custom_description = custom_description
        self._none_safe = none_safe

    def apply(self, value: Any) -> Any:
        if self._none_safe:
            return self._fn(value)
        if value is None:
            return None
        return self._fn(value)

    def _apply(self, value: Any) -> Any:
        return self._fn(value)

    @property
    def name(self) -> str:
        return self._custom_name

    @property
    def description(self) -> str:
        return self._custom_description or self._custom_name


# ── Helpers ─────────────────────────────────────────────────────


def _abbrev(n: float) -> str:
    """Abbreviate a number for display in descriptions."""
    if n == float("inf"):
        return "∞"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.0f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}k"
    return str(int(n))


def apply_transform_chain(
    transform: Transform,
    values: list[Any],
    all_rows: list[dict[str, Any]] | None = None,
) -> list[Any]:
    """
    Apply a transform (possibly chained, possibly dataset-aware) to a list of values.

    For simple transforms, applies per-element.
    For dataset-aware transforms, calls apply_batch().
    For chains containing dataset-aware steps, splits at boundaries.
    """
    if not isinstance(transform, ChainedTransform):
        if transform.is_dataset_aware:
            return transform.apply_batch(values)
        return [transform.apply(v) for v in values]

    # Split chain into segments around dataset-aware steps
    steps = transform.steps
    current_values = list(values)

    for step in steps:
        if step.is_dataset_aware:
            current_values = step.apply_batch(current_values)
        else:
            current_values = [step.apply(v) for v in current_values]

    return current_values
