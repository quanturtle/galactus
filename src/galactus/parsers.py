"""Per-source HTML-preservation policies used by scrapers during bronze-store cleaning.

A Parser is the declarative "what to keep" on a page: the allowed HTML attribute
whitelist, an optional script-content regex to preserve, and strip rules for tags
and classes. Domains declare defaults in code; per-source overrides live in the
source's YAML under a `parser:` section.
"""

import re
from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Parser:
    """Preservation policy for a single source's bronze HTML cleaning."""

    source: str
    allowed_attrs: frozenset[str]
    keep_script_re: re.Pattern | None = None
    strip_tags: tuple[str, ...] = ()
    strip_classes: tuple[str, ...] = ()


class ParserPolicyRegistry:
    """Merges domain-wide defaults with per-source YAML `parser:` overrides.

    Per-source values win over defaults. Fields supported in the `parser:`
    section: allowed_attrs, keep_script_re (str -> re.Pattern), strip_tags,
    strip_classes. YAML sequences are coerced to tuples / frozenset to match
    the immutable Parser dataclass.
    """

    def __init__(self, policies: dict[str, Parser]) -> None:
        self._policies = policies

    @classmethod
    def from_configs(
        cls,
        config_dir: Path | str,
        *,
        defaults: dict,
    ) -> "ParserPolicyRegistry":
        config_dir = Path(config_dir)
        policies: dict[str, Parser] = {}
        for yaml_file in sorted(config_dir.glob("*.yml")):
            with yaml_file.open() as f:
                data = yaml.safe_load(f) or {}
            source = data.get("source", yaml_file.stem)
            override = data.get("parser") or {}
            policies[source] = _build_parser(source, defaults, override)
        return cls(policies)

    def get(self, source: str) -> Parser:
        try:
            return self._policies[source]
        except KeyError as e:
            raise KeyError(f"No parser registered for source: {source}") from e


def _build_parser(source: str, defaults: dict, override: dict) -> Parser:
    merged = {**defaults, **override}
    kwargs: dict = {"source": source}
    if "allowed_attrs" in merged:
        kwargs["allowed_attrs"] = frozenset(merged["allowed_attrs"])
    if "keep_script_re" in merged:
        v = merged["keep_script_re"]
        kwargs["keep_script_re"] = v if isinstance(v, re.Pattern) else re.compile(v)
    if "strip_tags" in merged:
        kwargs["strip_tags"] = tuple(merged["strip_tags"])
    if "strip_classes" in merged:
        kwargs["strip_classes"] = tuple(merged["strip_classes"])
    return Parser(**kwargs)
