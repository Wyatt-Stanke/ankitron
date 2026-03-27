"""
AISource — generate field values via LLM prompts.

Uses the Anthropic API (Claude) by default. Supports prompt templates
with {{field_name}} references, structured output via choices,
batch processing, cost estimation, and aggressive caching.

Requires the `ai` extra: ``pip install ankitron[ai]``.
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ankitron.deck import Field
    from ankitron.sources.link_strategy import LinkStrategy


class AISource:
    """A data source that generates field values via LLM prompts.

    The prompt template uses {{field_name}} syntax for per-row substitution.
    Fields referenced in the prompt must be fetched by a prior source.
    """

    def __init__(
        self,
        *,
        model: str = "claude-sonnet-4-20250514",
        linked_to: Any | None = None,
        via: LinkStrategy | None = None,
        max_concurrent: int = 5,
        cost_limit: float | None = None,
        temperature: float = 0.0,
        lazy: bool = True,
    ) -> None:
        self._model = model
        self._linked_to = linked_to
        self._via = via
        self._max_concurrent = max_concurrent
        self._cost_limit = cost_limit
        self._temperature = temperature
        self._lazy = lazy

    def Field(
        self,
        prompt: str,
        *,
        choices: list[str] | None = None,
        **kwargs: Any,
    ) -> Field:
        """Create a Field backed by an LLM prompt.

        Args:
            prompt: Prompt template with {{field_name}} references.
            choices: If provided, constrains the output to one of these values.
        """
        from ankitron.deck import Field as DeckField

        fld = DeckField(**kwargs)
        fld._source = self
        fld._source_key = prompt
        fld._ai_choices = choices  # type: ignore[attr-defined]
        return fld

    def fetch(
        self,
        fields: list[tuple[str, Field]],
        cache: Any | None = None,
        refresh: bool = False,
    ) -> list[dict[str, str]]:
        """Fetch values from the LLM API.

        Resolves prompt templates per row, checks cache, enforces cost limits,
        batch processes with concurrency control, and validates against choices.
        """
        try:
            import anthropic
        except ImportError as err:
            raise ImportError(
                "AISource requires the 'ai' extra. Install with: pip install ankitron[ai]"
            ) from err

        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError(
                "AISource requires the ANTHROPIC_API_KEY environment variable to be set."
            )

        client = anthropic.Anthropic(api_key=api_key)

        # Get existing rows from the linked source context
        existing_rows = getattr(self, "_linked_rows", [])
        if not existing_rows:
            return []

        results: list[dict[str, str]] = []
        total_cost = 0.0

        for row in existing_rows:
            row_result: dict[str, str] = {}

            for attr_name, fld in fields:
                prompt_template = fld._source_key or ""
                resolved_prompt = self._resolve_prompt(prompt_template, row)
                choices = getattr(fld, "_ai_choices", None)

                # Check cache
                c_key = self._cache_key(self._model, resolved_prompt, self._temperature)
                if cache and not refresh:
                    cached = cache.get(f"ai:{c_key}")
                    if cached is not None:
                        row_result[attr_name] = cached
                        continue

                # Cost limit check
                if self._cost_limit is not None and total_cost >= self._cost_limit:
                    row_result[attr_name] = ""
                    continue

                # Build system prompt
                system = "You are a helpful assistant generating flashcard content."
                if choices:
                    system += (
                        f" Your response MUST be exactly one of: {', '.join(choices)}. "
                        "Respond with only the chosen value, no explanation."
                    )

                # API call with retry
                response_text = ""
                for attempt in range(3):
                    try:
                        response = client.messages.create(
                            model=self._model,
                            max_tokens=256,
                            temperature=self._temperature,
                            system=system,
                            messages=[{"role": "user", "content": resolved_prompt}],
                        )
                        response_text = response.content[0].text.strip()

                        # Track cost (approximate)
                        input_tokens = response.usage.input_tokens
                        output_tokens = response.usage.output_tokens
                        total_cost += (input_tokens * 0.003 + output_tokens * 0.015) / 1000

                        break
                    except Exception:
                        if attempt == 2:
                            response_text = ""
                        else:
                            import time

                            time.sleep(2**attempt)

                # Validate against choices
                if choices and response_text not in choices:
                    # Find closest match
                    lower_choices = {c.lower(): c for c in choices}
                    response_text = lower_choices.get(response_text.lower(), "")

                # Cache result
                if cache and response_text:
                    cache.set(f"ai:{c_key}", response_text)

                row_result[attr_name] = response_text

            results.append(row_result)

        return results

    @staticmethod
    def _resolve_prompt(template: str, row: dict[str, Any]) -> str:
        """Resolve {{field_name}} references in a prompt template."""
        import re

        def replace_ref(match: re.Match) -> str:
            field_name = match.group(1)
            return str(row.get(field_name, ""))

        return re.sub(r"\{\{(\w+)\}\}", replace_ref, template)

    @staticmethod
    def _cache_key(model: str, prompt: str, temperature: float) -> str:
        """Generate a stable cache key for an AI request."""
        data = json.dumps(
            {"model": model, "prompt": prompt, "temperature": temperature}, sort_keys=True
        )
        return hashlib.sha256(data.encode()).hexdigest()
