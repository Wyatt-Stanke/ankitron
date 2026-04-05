"""
ankitron.ai — AI integration for generating flashcard content.

Provides system prompt presets, caching, chunked/batched API calls,
and cost management for LLM-powered field generation.
"""

from ankitron.ai.prompts import SystemPrompt
from ankitron.ai.types import AIExample, BatchPolicy, CardSchema, ChunkConfig

__all__ = [
    "AIExample",
    "BatchPolicy",
    "CardSchema",
    "ChunkConfig",
    "SystemPrompt",
]
