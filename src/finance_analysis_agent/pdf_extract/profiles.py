"""Template profile registry for statement parsing heuristics."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TemplateProfile:
    name: str
    default_currency: str = "USD"
    default_pending_status: str = "posted"


class TemplateProfileRegistry:
    """Resolve template hints to parsing profiles."""

    def __init__(self, profiles: dict[str, TemplateProfile] | None = None) -> None:
        self._profiles = profiles or {"generic": TemplateProfile(name="generic")}

    def resolve(self, template_hint: str | None) -> TemplateProfile:
        if not template_hint:
            return self._profiles["generic"]

        key = template_hint.strip().lower()
        return self._profiles.get(key, self._profiles["generic"])


def build_default_profile_registry() -> TemplateProfileRegistry:
    return TemplateProfileRegistry()
