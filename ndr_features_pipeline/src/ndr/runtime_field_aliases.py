"""Helpers for canonical-field migration with deterministic legacy warnings."""

from __future__ import annotations

from ndr.logging.logger import get_logger


LOGGER = get_logger(__name__)


def resolve_with_legacy_alias(
    *,
    canonical_value: str | None,
    legacy_value: str | None,
    canonical_name: str,
    legacy_name: str,
    context: str,
) -> str:
    """Resolve canonical value with legacy fallback and deterministic warning log."""
    canonical = (canonical_value or "").strip()
    if canonical:
        return canonical

    legacy = (legacy_value or "").strip()
    if legacy:
        LOGGER.warning(
            "LegacyFieldNameUsed context=%s legacy_field=%s canonical_field=%s",
            context,
            legacy_name,
            canonical_name,
        )
    return legacy
