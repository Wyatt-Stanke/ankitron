"""Shared utilities for Anthropic API access."""

from __future__ import annotations

import json
import os
import re
import time
from typing import Any, Literal


# ---------------------------------------------------------------------------
# Cost constants
# ---------------------------------------------------------------------------

_INPUT_COST_PER_TOKEN: float = 0.003 / 1000   # $0.003 per 1 k input tokens
_OUTPUT_COST_PER_TOKEN: float = 0.015 / 1000  # $0.015 per 1 k output tokens
_BATCH_DISCOUNT: float = 0.5


def calculate_cost(tokens_in: int, tokens_out: int, *, batch: bool = False) -> float:
    """Calculate Anthropic API cost in USD.

    Args:
        tokens_in: Number of input tokens consumed.
        tokens_out: Number of output tokens produced.
        batch: If True, apply the 50 % Message Batches discount.
    """
    cost = tokens_in * _INPUT_COST_PER_TOKEN + tokens_out * _OUTPUT_COST_PER_TOKEN
    return cost * _BATCH_DISCOUNT if batch else cost


# ---------------------------------------------------------------------------
# Client setup
# ---------------------------------------------------------------------------

def require_anthropic_client(extra_name: str = "ai") -> Any:
    """Import anthropic, validate the API key, and return an initialised client.

    Args:
        extra_name: The ankitron extra to mention in the ImportError message.

    Returns:
        An ``anthropic.Anthropic`` client instance.

    Raises:
        ImportError: If the ``anthropic`` package is not installed.
        RuntimeError: If ``ANTHROPIC_API_KEY`` is not set.
    """
    try:
        import anthropic
    except ImportError as err:
        raise ImportError(
            f"This feature requires the '{extra_name}' extra. "
            f"Install with: pip install ankitron[{extra_name}]"
        ) from err

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY environment variable is required.")

    return anthropic.Anthropic(api_key=api_key)


# ---------------------------------------------------------------------------
# API call helpers
# ---------------------------------------------------------------------------

def call_api_with_retry(
    client: Any,
    *,
    model: str,
    system: str,
    messages: list[dict[str, str]],
    max_tokens: int = 1024,
    temperature: float = 0.0,
    on_failure: Literal["raise", "empty"] = "empty",
) -> tuple[str, int, int]:
    """Call the Anthropic messages API with exponential-backoff retry.

    Args:
        client: An ``anthropic.Anthropic`` client instance.
        model: Model identifier string.
        system: System prompt text.
        messages: List of ``{"role": ..., "content": ...}`` dicts.
        max_tokens: Maximum tokens to generate.
        temperature: Sampling temperature.
        on_failure: What to do on the final failed attempt.
            ``"raise"`` re-raises the exception; ``"empty"`` returns an
            empty string.

    Returns:
        A ``(response_text, tokens_in, tokens_out)`` tuple.
    """
    response_text = ""
    tokens_in = tokens_out = 0

    for attempt in range(3):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
                system=system,
                messages=messages,
            )
            response_text = response.content[0].text.strip()
            tokens_in = response.usage.input_tokens
            tokens_out = response.usage.output_tokens
            break
        except Exception:
            if attempt == 2:
                if on_failure == "raise":
                    raise
                response_text = ""
            else:
                time.sleep(2**attempt)

    return response_text, tokens_in, tokens_out


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def parse_json_response(text: str) -> list[Any]:
    """Parse a JSON array from an LLM response, tolerating markdown fences.

    Strips leading/trailing whitespace, removes ```...``` fences, strips
    trailing commas before ``]`` or ``}``, then attempts ``json.loads``.

    Returns an empty list on any parse failure.
    """
    cleaned = text.strip()
    if cleaned.startswith("```"):
        first_nl = cleaned.index("\n") if "\n" in cleaned else 3
        cleaned = cleaned[first_nl + 1:]
        cleaned = cleaned.removesuffix("```").strip()

    cleaned = re.sub(r",\s*([}\]])", r"\1", cleaned)

    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass
    return []
