import hashlib


def generate_id_in_range(key: str) -> int:
    """Generate a deterministic integer ID in genanki's recommended range [1<<30, 1<<31)."""
    hash_hex = hashlib.sha256(key.encode()).hexdigest()
    raw = int(hash_hex, 16)
    return (raw % (1 << 30)) + (1 << 30)


def generate_note_id(deck_qualname: str, pk_value: str) -> int:
    """Generate a deterministic note ID from the deck class qualname and the primary key value."""
    key = f"{deck_qualname}::{pk_value}"
    return generate_id_in_range(key)


def generate_deck_id(deck_qualname: str) -> int:
    """Generate a deterministic deck ID from the deck class qualname."""
    key = f"deck::{deck_qualname}"
    return generate_id_in_range(key)


def generate_model_id(deck_qualname: str) -> int:
    """Generate a deterministic model ID from the deck class qualname."""
    key = f"model::{deck_qualname}"
    return generate_id_in_range(key)
