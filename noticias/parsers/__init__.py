import sys

from galactus.discovery import ParserRegistry

_registry = ParserRegistry.from_package(sys.modules[__name__])

HTML_PARSERS = _registry.html
API_PARSERS = _registry.api
parse_snapshot = _registry.parse_snapshot
parse_api_response = _registry.parse_api_response
