from __future__ import annotations

from finance_analysis_agent.pdf_extract.profiles import TemplateProfile, TemplateProfileRegistry


def test_registry_injects_generic_profile_when_missing() -> None:
    registry = TemplateProfileRegistry(
        profiles={
            "chime": TemplateProfile(name="chime", default_currency="USD"),
        }
    )

    resolved_default = registry.resolve(None)
    resolved_unknown = registry.resolve("unknown-issuer")
    resolved_known = registry.resolve("chime")

    assert resolved_default.name == "generic"
    assert resolved_unknown.name == "generic"
    assert resolved_known.name == "chime"
