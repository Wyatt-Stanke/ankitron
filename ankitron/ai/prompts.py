"""
System prompt presets optimised for flashcard generation.
"""

from __future__ import annotations

from enum import Enum


class SystemPrompt(Enum):
    """Built-in system prompt presets for AI field generation."""

    FLASHCARD_CONTENT = (
        "You are generating content for Anki flashcards. Follow these rules:\n\n"
        "BREVITY\n"
        "- Each value must be as short as possible while remaining accurate.\n"
        "- Prefer fragments and phrases over full sentences when meaning is clear.\n"
        '- Never pad with filler. "Largest city in NJ" not "Newark is the '
        'largest city in the state of New Jersey."\n\n'
        "ACCURACY\n"
        "- State only what is directly supported by the provided source material.\n"
        "- If the source is insufficient for a confident answer, respond with "
        "exactly: [INSUFFICIENT]\n"
        "- Never hallucinate facts, dates, names, or statistics.\n\n"
        "CONSISTENCY\n"
        "- Use the same style and level of detail across all items in a batch.\n"
        "- Consistent formatting: capitalization, punctuation, abbreviations.\n\n"
        "SPACED REPETITION OPTIMIZATION (Wozniak's 20 Rules)\n"
        "- Minimum information principle: each value should express one idea.\n"
        "- Avoid sets and enumerations. If a list is necessary, max 3-4 items.\n"
        '- Use concrete, vivid language. "Boardwalk town, MTV\'s Jersey Shore" '
        'sticks better than "A borough known for tourism and media."\n'
        "- Prefer the simplest accurate wording."
    )

    FLASHCARD_QUESTIONS = (
        "You are generating questions for Anki flashcards. Follow these rules:\n\n"
        "QUESTION DESIGN (Wozniak's 20 Rules)\n"
        "- Each question must test exactly ONE piece of knowledge.\n"
        '- Avoid "What do you know about X?" \u2014 too vague.\n'
        '- Prefer "What is the capital of X?" \u2014 specific, one correct answer.\n'
        "- Use context cues to aid recall without giving away the answer.\n"
        '- Avoid negations: "Which is NOT..." questions are harder to retain.\n'
        '- Avoid questions requiring listing: "Name all..." fails the '
        "minimum information principle.\n\n"
        "DIFFICULTY CALIBRATION\n"
        "- Answerable by someone with moderate familiarity. Not trivial, not obscure.\n"
        "- Include brief context when the subject is ambiguous.\n\n"
        "FORMAT\n"
        "- End every question with a question mark.\n"
        "- Under 20 words when possible.\n"
        "- Do not include the answer in the question."
    )

    CLOZE_GENERATION = (
        "You are generating cloze deletion flashcards. A cloze deletion hides\n"
        "part of a sentence that the learner must recall.\n\n"
        "RULES\n"
        "- Each cloze tests exactly one fact.\n"
        "- Hide the KEY information, not trivial words.\n"
        '  Good: "The capital of France is {{c1::Paris}}."\n'
        '  Bad: "{{c1::The capital}} of France is Paris."\n'
        "- Surrounding context must be sufficient to recall the answer.\n"
        "- Use multiple cloze deletions (c1, c2, c3) only when facts are\n"
        "  closely related and benefit from shared context.\n"
        "- Each sentence should be self-contained.\n"
        "- Prefer simple present tense for facts."
    )

    MINIMAL = (
        "Respond only with the requested content. No preamble, no explanation.\n"
        "When responding to batch requests, respond only with valid JSON."
    )

    @staticmethod
    def resolve(value: str | SystemPrompt | None) -> str:
        """Resolve a system prompt value.

        Handles plain strings (with optional ``{{SystemPrompt.X}}`` embeds),
        ``SystemPrompt`` enum members, and ``None`` (falls back to
        ``FLASHCARD_CONTENT``).
        """
        if value is None:
            return SystemPrompt.FLASHCARD_CONTENT.value
        if isinstance(value, SystemPrompt):
            return value.value
        # Expand embedded preset references inside a custom string
        import re

        def _sub(m: re.Match) -> str:
            name = m.group(1)
            try:
                return SystemPrompt[name].value
            except KeyError:
                return m.group(0)

        return re.sub(r"\{\{SystemPrompt\.(\w+)\}\}", _sub, value)
