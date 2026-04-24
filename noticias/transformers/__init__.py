import sys

from galactus.discovery import TransformerRegistry

_registry = TransformerRegistry.from_package(sys.modules[__name__])

HTML_TRANSFORMERS = _registry.html
API_TRANSFORMERS = _registry.api
transform_snapshot = _registry.transform_snapshot
transform_api_response = _registry.transform_api_response
