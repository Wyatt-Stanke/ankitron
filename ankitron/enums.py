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


class FieldRule(Enum):
    """Per-field expectation about data completeness."""

    REQUIRED = "required"  # fetch() raises error if missing
    EXPECTED = "expected"  # fetch() logs warning if missing
    OPTIONAL = "optional"  # Missing values are expected and normal


class Severity(Enum):
    """Severity level for validators."""

    ERROR = "error"  # Validation failure prevents export
    WARN = "warn"  # Warning is logged, export proceeds


class MediaType(Enum):
    """Declares the type of media a field contains."""

    IMAGE = "image"
    AUDIO = "audio"


class MediaFormat(Enum):
    """Target format for media conversion."""

    PNG = "png"
    JPEG = "jpeg"
    SVG = "svg"
    WEBP = "webp"
    MP3 = "mp3"
    OGG = "ogg"
