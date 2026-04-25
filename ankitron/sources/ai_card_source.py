"""
AICardSource — generate multiple flashcard rows from a single input.

Unlike ``AISource`` (one value per existing row), ``AICardSource``
takes a document or set of inputs and produces *many* rows.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Any

from ankitron.ai.cache import AICache
from ankitron.ai.prompts import SystemPrompt
from ankitron.ai.types import BatchPolicy, CardSchema
from ankitron.ai.utils import (
    calculate_cost,
    call_api_with_retry,
    parse_json_response,
    require_anthropic_client,
)

if TYPE_CHECKING:
    from ankitron.deck import Field


class AICardSource:
    """A source that generates multiple rows from a single input via LLM.

    Args:
        model: Anthropic model identifier.
        input: A single source whose content seeds the generation.
        inputs: Mapping of label → source for multi-source generation.
        version: Cache invalidation version number.
        system: System prompt (string or ``SystemPrompt`` preset).
        prompt: Instruction for the AI.
        schema: ``CardSchema`` describing expected output fields.
        batch: Batch processing policy.
        temperature: Sampling temperature.
    """

    def __init__(
        self,
        *,
        model: str = "claude-sonnet-4-20250514",
        input: Any | None = None,  # noqa: A002
        inputs: dict[str, Any] | None = None,
        version: int = 1,
        system: str | SystemPrompt | None = None,
        prompt: str = "",
        schema: CardSchema | None = None,
        batch: BatchPolicy = BatchPolicy.DISABLED,
        temperature: float = 0.0,
    ) -> None:
        self._model = model
        self._input = input
        self._inputs = inputs
        self._version = version
        self._system = system
        self._prompt = prompt
        self._schema = schema
        self._batch = batch
        self._temperature = temperature
        self._ai_cache = AICache()

    def Field(self, field_name: str, **kwargs: Any) -> Field:
        """Create a Field bound to one column of the generated schema."""
        from ankitron.deck import Field as DeckField

        fld = DeckField(**kwargs)
        fld._source = self
        fld._source_key = field_name
        return fld

    def fetch(
        self,
        fields: list[tuple[str, Field]],
        cache: Any | None = None,
        refresh: bool = False,
    ) -> list[dict[str, str]]:
        """Generate rows via LLM call (or return cached result)."""
        client = require_anthropic_client("ai")

        deck_class = getattr(self, "_deck_qualname", "unknown")
        source_name = getattr(self, "_source_attr", "ai")

        # Gather input content
        input_text = self._gather_input_text()
        input_hash = hashlib.sha256(input_text.encode()).hexdigest()

        # Check cache
        if not refresh:
            cached_rows = self._ai_cache.get_card_source(
                deck_class,
                source_name,
                self._version,
                input_hash,
            )
            if cached_rows is not None:
                return self._filter_fields(cached_rows, fields)

        # Build the generation prompt
        system_text = SystemPrompt.resolve(self._system)
        user_prompt = self._build_generation_prompt(input_text)

        response_text, tokens_in, tokens_out = call_api_with_retry(
            client,
            model=self._model,
            system=system_text,
            messages=[{"role": "user", "content": user_prompt}],
            max_tokens=4096,
            temperature=self._temperature,
            on_failure="raise",
        )

        # Parse the generated rows
        rows = self._parse_response(response_text)
        cost = calculate_cost(tokens_in, tokens_out)

        # Cache the full result set
        self._ai_cache.put_card_source(
            deck_class=deck_class,
            source_name=source_name,
            version=self._version,
            input_hash=input_hash,
            output_rows=rows,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            cost_usd=cost,
        )

        return self._filter_fields(rows, fields)

    # -- internal helpers -----------------------------------------------------

    def _gather_input_text(self) -> str:
        """Collect input text from the configured source(s)."""
        if self._input is not None:
            # Single source — read its content
            content = getattr(self._input, "_content", None)
            if content:
                return content
            # Fallback: try to read from file path
            path = getattr(self._input, "_path", None)
            if path:
                from pathlib import Path

                return Path(path).read_text(encoding="utf-8")
            return str(self._input)
        if self._inputs:
            parts = []
            for label, src in self._inputs.items():
                content = getattr(src, "_content", None)
                if content:
                    parts.append(f"[{label}]\n{content}")
                else:
                    parts.append(f"[{label}]\n{src}")
            return "\n\n---\n\n".join(parts)
        return ""

    def _build_generation_prompt(self, input_text: str) -> str:
        """Build the full user prompt including schema instructions."""
        lines = [self._prompt.strip()]

        if self._schema:
            lines.append("")
            lines.append("Output schema — each card must have:")
            for fname, fdesc in self._schema.fields.items():
                lines.append(f"- {fname}: {fdesc}")
            lines.append("")
            lines.append("Respond ONLY with a JSON array of objects matching the schema.")

        lines.append("")
        lines.append("---")
        lines.append(input_text)

        return "\n".join(lines)

    @staticmethod
    def _parse_response(text: str) -> list[dict[str, Any]]:
        """Parse a JSON array response, tolerating markdown fences."""
        return parse_json_response(text)  # type: ignore[return-value]

    @staticmethod
    def _filter_fields(
        rows: list[dict[str, Any]],
        fields: list[tuple[str, Any]],
    ) -> list[dict[str, str]]:
        """Filter generated rows to only include requested field columns."""
        result: list[dict[str, str]] = []
        for row in rows:
            filtered: dict[str, str] = {}
            for attr_name, fld in fields:
                key = fld._source_key or attr_name
                filtered[attr_name] = str(row.get(key, ""))
            result.append(filtered)
        return result
