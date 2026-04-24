"""supermercados — domain-wide parser (HTML preservation) defaults."""

import re

DEFAULT_PARSER_KWARGS = {
    "allowed_attrs": frozenset({
        "id", "class", "href", "src", "alt", "content", "property",
        "name", "type", "data-product-id", "data-product-price", "data-modo_venta",
    }),
    "keep_script_re": re.compile(r"var\s+data\s*=\s*\{"),
}
