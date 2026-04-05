"""
AI data types — configuration objects for AI-powered field generation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


@dataclass(frozen=True)
class AIExample:
    """A few-shot example for AI field generation.

    Attributes:
        input: Mapping of field names to example input values.
        output: The expected output string the AI should produce.
    """

    input: dict[str, str]
    output: str


@dataclass
class ChunkConfig:
    """Controls how rows are batched into single API calls.

    Attributes:
        default_size: Number of rows per chunk.
        max_input_tokens: Hard cap on estimated input tokens per chunk.
    """

    default_size: int = 20
    max_input_tokens: int = 8000


@dataclass
class CardSchema:
    """Schema definition for AICardSource output.

    Attributes:
        fields: Mapping of field name → description for the AI.
        pk: Which field in ``fields`` serves as the primary key.
        group_by: Optional field to partition inputs before generating.
    """

    fields: dict[str, str]
    pk: str | None = None
    group_by: str | None = None


class BatchPolicy(Enum):
    """Controls whether AI calls use the Message Batches API."""

    DISABLED = "disabled"  # always real-time (default)
    PREFER = "prefer"  # batch when possible, real-time fallback
    REQUIRE = "require"  # only batch, fail if unavailable
    AUTO = "auto"  # batch if >50 values need generation


@dataclass
class AICostEstimate:
    """Pre-flight cost estimate for AI generation.

    Attributes:
        total_values: Total AI field values to generate.
        cached_values: Values already cached.
        uncached_values: Values needing generation.
        estimated_calls: Number of API calls (after chunking).
        estimated_cost_realtime: Real-time cost estimate (USD).
        estimated_cost_batch: Batch cost estimate (USD, 50% discount).
    """

    total_values: int = 0
    cached_values: int = 0
    uncached_values: int = 0
    estimated_calls: int = 0
    estimated_cost_realtime: float = 0.0
    estimated_cost_batch: float = 0.0


@dataclass
class AIBudgetConfig:
    """Budget guardrails for AI spending.

    Attributes:
        auto_approve_below: Skip confirmation if cost below this (USD).
        fail_above: Hard fail if cost exceeds this (USD).
        confirm_between: Prompt user for costs between thresholds.
        monthly_limit: Monthly spending cap (USD).
        alert_at: Alert when monthly spend exceeds this (USD).
    """

    auto_approve_below: float = 2.00
    fail_above: float = 20.00
    confirm_between: bool = True
    monthly_limit: float = 20.00
    alert_at: float = 15.00


@dataclass
class AIFieldConfig:
    """Internal configuration for an AI-backed field.

    Attributes:
        prompt: Prompt template with ``{{field_name}}`` references.
        version: Cache invalidation version number.
        system: System prompt (string or SystemPrompt preset).
        choices: Constrain output to one of these values.
        chunk_size: Rows per chunk (overrides source-level default).
        examples: Few-shot examples.
        batch: Batch policy override for this field.
    """

    prompt: str = ""
    version: int = 1
    system: Any = None
    choices: list[str] | None = None
    chunk_size: int | None = None
    examples: list[AIExample] = field(default_factory=list)
    batch: BatchPolicy | None = None
