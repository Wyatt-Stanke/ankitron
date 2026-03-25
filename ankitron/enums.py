from enum import Enum


class AnkiTemplate(Enum):
    """Selects which built-in Anki note type to use."""

    BASIC = "Basic"
    BASIC_REVERSED = "Basic (and reversed card)"
    BASIC_OPTIONAL_REVERSED = "Basic (optional reversed card)"
    BASIC_TYPE_ANSWER = "Basic (type in the answer)"
    CLOZE = "Cloze"


class FieldKind(Enum):
    """Declares the type of content a field contains."""

    TEXT = "text"
    IMAGE = "image"
    AUDIO = "audio"
    CLOZE = "cloze"


class PKStrategy(Enum):
    """Controls how the primary key value is derived for note ID generation."""

    FIELD_VALUE = "field_value"
    SOURCE_ID = "source_id"
