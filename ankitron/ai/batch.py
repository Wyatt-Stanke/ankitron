"""
Batch processing for AI fields via Anthropic's Message Batches API.

Provides 50% cost reduction for asynchronous processing with results
within 24 hours.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ankitron.ai.cache import AICache


@dataclass
class BatchRequest:
    """A single request within a batch."""

    custom_id: str
    deck_class: str
    row_pk: str
    field_name: str
    field_version: int
    input_hash: str
    prompt_template: str
    resolved_prompt: str
    resolved_inputs: dict[str, str]
    messages: list[dict[str, str]]
    model: str = "claude-sonnet-4-20250514"
    max_tokens: int = 1024


@dataclass
class BatchResult:
    """Result of a batch submission or collection."""

    batch_id: str
    total_requests: int = 0
    completed: int = 0
    failed: int = 0
    status: str = "unknown"
    results: list[dict[str, Any]] = field(default_factory=list)


def submit_batch(
    requests: list[BatchRequest],
) -> BatchResult:
    """Submit a batch of AI requests to Anthropic's Message Batches API."""
    try:
        import anthropic
    except ImportError as err:
        raise ImportError(
            "Batch processing requires the 'ai' extra. Install with: pip install ankitron[ai]"
        ) from err

    import os

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY environment variable is required.")

    client = anthropic.Anthropic(api_key=api_key)

    batch_requests = [
        {
            "custom_id": req.custom_id,
            "params": {
                "model": req.model,
                "max_tokens": req.max_tokens,
                "messages": req.messages,
            },
        }
        for req in requests
    ]

    batch = client.messages.batches.create(requests=batch_requests)

    return BatchResult(
        batch_id=batch.id,
        total_requests=len(requests),
        status="submitted",
    )


def check_batch_status(batch_id: str) -> BatchResult:
    """Check the status of a submitted batch."""
    try:
        import anthropic
    except ImportError as err:
        raise ImportError("Batch processing requires the 'ai' extra.") from err

    import os

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
    batch = client.messages.batches.retrieve(batch_id)

    return BatchResult(
        batch_id=batch.id,
        total_requests=batch.request_counts.processing
        + batch.request_counts.succeeded
        + batch.request_counts.errored,
        completed=batch.request_counts.succeeded,
        failed=batch.request_counts.errored,
        status=batch.processing_status,
    )


def collect_batch_results(
    batch_id: str,
    requests: list[BatchRequest] | None = None,
    cache: AICache | None = None,
    model: str = "claude-sonnet-4-20250514",
) -> BatchResult:
    """Collect completed batch results and optionally store in the AI cache.

    If *requests* and *cache* are provided, results are cached using the
    request metadata.  Otherwise, results are returned without caching.
    """
    try:
        import anthropic
    except ImportError as err:
        raise ImportError("Batch processing requires the 'ai' extra.") from err

    import os

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))

    # Build lookup from custom_id → BatchRequest (if available)
    request_map = {req.custom_id: req for req in requests} if requests else {}

    results_iter = client.messages.batches.results(batch_id)
    collected = 0
    failed = 0
    result_list: list[dict[str, Any]] = []

    for result in results_iter:
        custom_id = result.custom_id

        if result.result.type == "succeeded":
            message = result.result.message
            output = message.content[0].text.strip()
            tokens_in = message.usage.input_tokens
            tokens_out = message.usage.output_tokens
            cost = (tokens_in * 0.003 + tokens_out * 0.015) / 1000 * 0.5  # 50% discount

            result_list.append(
                {
                    "custom_id": custom_id,
                    "output": output,
                    "tokens_in": tokens_in,
                    "tokens_out": tokens_out,
                    "cost_usd": cost,
                }
            )

            # Cache if we have request metadata
            req = request_map.get(custom_id)
            if req is not None and cache is not None:
                cache.put(
                    deck_class=req.deck_class,
                    row_pk=req.row_pk,
                    field_name=req.field_name,
                    field_version=req.field_version,
                    input_hash=req.input_hash,
                    model=model,
                    prompt_template=req.prompt_template,
                    resolved_prompt=req.resolved_prompt,
                    resolved_inputs=req.resolved_inputs,
                    output=output,
                    tokens_in=tokens_in,
                    tokens_out=tokens_out,
                    cost_usd=cost,
                )
            collected += 1
        else:
            failed += 1

    return BatchResult(
        batch_id=batch_id,
        total_requests=max(len(request_map), collected + failed),
        completed=collected,
        failed=failed,
        status="collected",
        results=result_list,
    )


def cancel_batch(batch_id: str) -> None:
    """Cancel a pending batch."""
    try:
        import anthropic
    except ImportError as err:
        raise ImportError("Batch processing requires the 'ai' extra.") from err

    import os

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
    client.messages.batches.cancel(batch_id)


def wait_for_batch(
    batch_id: str,
    poll_interval: int = 60,
    timeout: int = 86400,
) -> BatchResult:
    """Poll until a batch completes or times out."""
    start = time.monotonic()
    while True:
        result = check_batch_status(batch_id)
        if result.status in ("ended", "completed", "expired", "canceled"):
            return result
        elapsed = time.monotonic() - start
        if elapsed >= timeout:
            result.status = "timeout"
            return result
        time.sleep(poll_interval)
