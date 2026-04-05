"""
AISource — generate field values via LLM prompts.

Uses the Anthropic API (Claude) by default. Supports:

- Version-gated caching (never re-runs unless version bumped or inputs change)
- Chunked requests (multiple rows per API call)
- System prompt presets and custom system prompts
- Few-shot examples
- Cost estimation and confirmation
- ``[INSUFFICIENT]`` convention
- Batch processing via Message Batches API

Requires the ``ai`` extra: ``pip install ankitron[ai]``.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import TYPE_CHECKING, Any

from ankitron.ai.cache import AICache, ai_cache_key
from ankitron.ai.prompts import SystemPrompt
from ankitron.ai.types import (
    AIExample,
    AIFieldConfig,
    BatchPolicy,
)

if TYPE_CHECKING:
    from ankitron.deck import Field


class AISource:
    """A data source that generates field values via LLM prompts.

    The prompt template uses ``{{field_name}}`` syntax for per-row substitution.
    Fields referenced in the prompt must be fetched by a prior source.
    """

    def __init__(
        self,
        *,
        model: str = "claude-sonnet-4-20250514",
        linked_to: Any | None = None,
        via: Any | None = None,
        max_concurrent: int = 5,
        cost_limit: float | None = None,
        temperature: float = 0.0,
        lazy: bool = True,
        chunk_size: int = 20,
        batch: BatchPolicy = BatchPolicy.DISABLED,
        system: str | SystemPrompt | None = None,
    ) -> None:
        self._model = model
        self._linked_to = linked_to
        self._via = via
        self._max_concurrent = max_concurrent
        self._cost_limit = cost_limit
        self._temperature = temperature
        self._lazy = lazy
        self._chunk_size = chunk_size
        self._batch = batch
        self._system = system
        self._ai_cache = AICache()

    # -- Field factories ------------------------------------------------------

    def Field(
        self,
        prompt: str,
        *,
        version: int = 1,
        system: str | SystemPrompt | None = None,
        choices: list[str] | None = None,
        chunk_size: int | None = None,
        examples: list[AIExample] | None = None,
        batch: BatchPolicy | None = None,
        **kwargs: Any,
    ) -> Field:
        """Create a Field backed by an LLM prompt.

        Args:
            prompt: Prompt template with ``{{field_name}}`` references.
            version: Cache invalidation version. Bump to regenerate.
            system: System prompt override (string or ``SystemPrompt`` preset).
            choices: Constrain output to one of these values.
            chunk_size: Rows per chunk for this field.
            examples: Few-shot examples for this field.
            batch: Batch policy override for this field.
        """
        from ankitron.deck import Field as DeckField

        fld = DeckField(**kwargs)
        fld._source = self
        fld._source_key = prompt
        fld._ai_config = AIFieldConfig(  # type: ignore[attr-defined]
            prompt=prompt,
            version=version,
            system=system,
            choices=choices,
            chunk_size=chunk_size,
            examples=examples or [],
            batch=batch,
        )
        return fld

    def ExpandField(
        self,
        *,
        version: int = 1,
        input_fields: list[Any] | None = None,
        prompt: str = "",
        schema: dict[str, str] | None = None,
        per_row: int = 3,
        **kwargs: Any,
    ) -> Field:
        """Create a field that expands each row into multiple sub-rows.

        Each parent row produces ``per_row`` additional rows with fields
        defined by *schema*.
        """
        from ankitron.deck import Field as DeckField

        fld = DeckField(**kwargs)
        fld._source = self
        fld._source_key = prompt
        fld._ai_expand = {  # type: ignore[attr-defined]
            "version": version,
            "input_fields": input_fields or [],
            "schema": schema or {},
            "per_row": per_row,
        }
        return fld

    # -- fetch ----------------------------------------------------------------

    def fetch(
        self,
        fields: list[tuple[str, Field]],
        cache: Any | None = None,
        refresh: bool = False,
    ) -> list[dict[str, str]]:
        """Fetch values from the LLM API.

        Uses the SQLite AI cache (version-gated, input-hash-keyed).
        Falls back to chunked API calls for cache misses.
        """
        try:
            import anthropic
        except ImportError as err:
            raise ImportError(
                "AISource requires the 'ai' extra. Install with: pip install ankitron[ai]"
            ) from err

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("AISource requires the ANTHROPIC_API_KEY environment variable.")

        client = anthropic.Anthropic(api_key=api_key)

        existing_rows: list[dict[str, Any]] = getattr(self, "_linked_rows", [])
        if not existing_rows:
            return []

        deck_class = getattr(self, "_deck_qualname", "unknown")
        pk_attr = getattr(self, "_pk_field_attr", "")

        # Determine which (row, field) pairs need generation
        results: list[dict[str, str]] = [{} for _ in existing_rows]
        uncached: list[tuple[int, str, Field, str, str, dict[str, str]]] = []

        for attr_name, fld in fields:
            cfg: AIFieldConfig = getattr(
                fld,
                "_ai_config",
                AIFieldConfig(prompt=fld._source_key or ""),
            )
            for row_idx, row in enumerate(existing_rows):
                pk = row.get(pk_attr, str(row_idx))
                resolved_inputs = self._extract_inputs(cfg.prompt, row)
                ihash = ai_cache_key(cfg.version, resolved_inputs)

                if not refresh:
                    cached_val = self._ai_cache.get(
                        deck_class,
                        pk,
                        attr_name,
                        cfg.version,
                        ihash,
                    )
                    if cached_val is not None:
                        results[row_idx][attr_name] = cached_val
                        continue

                resolved_prompt = self._resolve_prompt(cfg.prompt, row)
                uncached.append((row_idx, attr_name, fld, pk, resolved_prompt, resolved_inputs))

        if not uncached:
            return results

        # Group uncached items by field for chunked processing
        by_field: dict[str, list[tuple[int, Field, str, str, dict[str, str]]]] = {}
        for row_idx, attr_name, fld, pk, resolved, inputs in uncached:
            by_field.setdefault(attr_name, []).append((row_idx, fld, pk, resolved, inputs))

        total_cost = 0.0

        for attr_name, items in by_field.items():
            fld = items[0][1]
            cfg = getattr(fld, "_ai_config", AIFieldConfig(prompt=fld._source_key or ""))
            system_text = SystemPrompt.resolve(cfg.system or self._system)
            csize = cfg.chunk_size or self._chunk_size

            if cfg.choices:
                system_text += (
                    f"\n\nYour response MUST be exactly one of: "
                    f"{', '.join(cfg.choices)}. "
                    "Respond with only the chosen value, no explanation."
                )

            # Process in chunks
            for chunk_start in range(0, len(items), csize):
                chunk = items[chunk_start : chunk_start + csize]

                if len(chunk) == 1:
                    # Single-item: direct call
                    row_idx, _fld, pk, resolved, inputs = chunk[0]
                    messages = self._build_messages(
                        cfg.examples,
                        resolved,
                        system_text,
                    )

                    if self._cost_limit is not None and total_cost >= self._cost_limit:
                        results[row_idx][attr_name] = ""
                        continue

                    response_text, cost = self._call_api(
                        client,
                        system_text,
                        messages,
                        cfg.choices,
                    )
                    total_cost += cost

                    if response_text == "[INSUFFICIENT]":
                        response_text = ""

                    results[row_idx][attr_name] = response_text

                    # Cache the result
                    ihash = ai_cache_key(cfg.version, inputs)
                    self._ai_cache.put(
                        deck_class=deck_class,
                        row_pk=pk,
                        field_name=attr_name,
                        field_version=cfg.version,
                        input_hash=ihash,
                        model=self._model,
                        prompt_template=cfg.prompt,
                        resolved_prompt=resolved,
                        resolved_inputs=inputs,
                        output=response_text,
                    )
                else:
                    # Multi-item chunk: batched JSON call
                    chunk_prompt = self._build_chunk_prompt(
                        attr_name,
                        cfg.prompt,
                        chunk,
                        pk_attr,
                    )
                    messages = self._build_messages(
                        cfg.examples,
                        chunk_prompt,
                        system_text,
                    )

                    if self._cost_limit is not None and total_cost >= self._cost_limit:
                        for row_idx, _, _pk, _, _ in chunk:
                            results[row_idx][attr_name] = ""
                        continue

                    response_text, cost = self._call_api(
                        client,
                        system_text,
                        messages,
                        None,
                    )
                    total_cost += cost

                    parsed = self._parse_chunk_response(response_text)
                    pk_to_value = {item.get("pk", ""): item.get("value", "") for item in parsed}

                    for row_idx, _fld, pk, resolved, inputs in chunk:
                        val = pk_to_value.get(pk, "")
                        if val == "[INSUFFICIENT]":
                            val = ""
                        results[row_idx][attr_name] = val

                        ihash = ai_cache_key(cfg.version, inputs)
                        self._ai_cache.put(
                            deck_class=deck_class,
                            row_pk=pk,
                            field_name=attr_name,
                            field_version=cfg.version,
                            input_hash=ihash,
                            model=self._model,
                            prompt_template=cfg.prompt,
                            resolved_prompt=resolved,
                            resolved_inputs=inputs,
                            output=val,
                        )

        return results

    # -- internal helpers -----------------------------------------------------

    def _call_api(
        self,
        client: Any,
        system: str,
        messages: list[dict[str, str]],
        choices: list[str] | None,
    ) -> tuple[str, float]:
        """Make a single API call with retry. Returns (text, cost_usd)."""
        response_text = ""
        cost = 0.0

        for attempt in range(3):
            try:
                response = client.messages.create(
                    model=self._model,
                    max_tokens=1024,
                    temperature=self._temperature,
                    system=system,
                    messages=messages,
                )
                response_text = response.content[0].text.strip()
                tokens_in = response.usage.input_tokens
                tokens_out = response.usage.output_tokens
                cost = (tokens_in * 0.003 + tokens_out * 0.015) / 1000
                break
            except Exception:
                if attempt == 2:
                    response_text = ""
                else:
                    import time

                    time.sleep(2**attempt)

        # Validate against choices
        if choices and response_text and response_text not in choices:
            lower_map = {c.lower(): c for c in choices}
            response_text = lower_map.get(response_text.lower(), "")

        return response_text, cost

    def _build_messages(
        self,
        examples: list[AIExample],
        user_content: str,
        system_text: str,
    ) -> list[dict[str, str]]:
        """Build message list with optional few-shot examples."""
        messages: list[dict[str, str]] = []

        # Add few-shot examples as user/assistant turns
        for ex in examples:
            input_text = ", ".join(f"{k}: {v}" for k, v in ex.input.items())
            messages.append({"role": "user", "content": input_text})
            messages.append({"role": "assistant", "content": ex.output})

        messages.append({"role": "user", "content": user_content})
        return messages

    def _build_chunk_prompt(
        self,
        field_name: str,
        prompt_template: str,
        chunk: list[tuple[int, Any, str, str, dict[str, str]]],
        pk_attr: str,
    ) -> str:
        """Build a chunked prompt for multiple rows."""
        lines = [
            f"Field: {field_name}",
            f"Instruction: {prompt_template}",
            "",
            "Respond ONLY with a JSON array. Each element must have "
            '"pk" (the item identifier) and "value" (your generated content).',
            "",
            "Items:",
        ]
        for i, (_, _, pk, _, inputs) in enumerate(chunk, 1):
            context = " ".join(f'{k}: "{v}"' for k, v in inputs.items())
            lines.append(f"{i}. [pk: {pk}] {context}")

        return "\n".join(lines)

    @staticmethod
    def _parse_chunk_response(text: str) -> list[dict[str, Any]]:
        """Parse a chunked JSON response, handling markdown fences and trailing commas."""
        cleaned = text.strip()
        # Strip markdown fences
        if cleaned.startswith("```"):
            first_nl = cleaned.index("\n") if "\n" in cleaned else 3
            cleaned = cleaned[first_nl + 1 :]
            cleaned = cleaned.removesuffix("```")
            cleaned = cleaned.strip()

        # Remove trailing commas before ] or }
        cleaned = re.sub(r",\s*([}\]])", r"\1", cleaned)

        try:
            parsed = json.loads(cleaned)
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass
        return []

    @staticmethod
    def _resolve_prompt(template: str, row: dict[str, Any]) -> str:
        """Resolve ``{{field_name}}`` references in a prompt template."""

        def replace_ref(match: re.Match) -> str:
            field_name = match.group(1)
            return str(row.get(field_name, ""))

        return re.sub(r"\{\{(\w+)\}\}", replace_ref, template)

    @staticmethod
    def _extract_inputs(template: str, row: dict[str, Any]) -> dict[str, str]:
        """Extract the resolved input values referenced by a prompt template."""
        refs = re.findall(r"\{\{(\w+)\}\}", template)
        return {ref: str(row.get(ref, "")) for ref in refs}

    @staticmethod
    def _cache_key(model: str, prompt: str, temperature: float) -> str:
        """Generate a stable cache key (legacy, for non-versioned lookups)."""
        data = json.dumps(
            {"model": model, "prompt": prompt, "temperature": temperature},
            sort_keys=True,
        )
        return hashlib.sha256(data.encode()).hexdigest()
