"""Shared dependency-checking utility for optional extras."""

from __future__ import annotations


def ensure_deps(packages: list[str], extra: str) -> None:
    """Check that all *packages* are importable, raising a helpful error if not.

    Args:
        packages: List of top-level import names to check.
        extra: The ankitron extra name to suggest in the error message.
    """
    missing: list[str] = []
    for pkg in packages:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)

    if missing:
        pkg_str = ", ".join(missing)
        raise ImportError(
            f"This feature requires: {pkg_str}. "
            f"Install with: pip install ankitron[{extra}]"
        )
