"""noticias — domain-wide parser (HTML preservation) defaults."""

import re

DEFAULT_PARSER_KWARGS = {
    "allowed_attrs": frozenset({
        "id", "class", "href", "src", "alt", "content", "property",
        "name", "type", "itemprop", "datetime", "rel",
    }),
    "keep_script_re": re.compile(r"(Fusion\.globalContent|var\s+data)\s*=\s*\{"),
}
