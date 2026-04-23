from galactus.transforms import api_responses as _generic
from supermercados.config import settings
from supermercados.parsers import API_PARSERS, parse_api_response
from supermercados.product import Product


async def run(source: str | None = None, *, chunk: int | None = None) -> int:
    return await _generic.run(
        entity_cls=Product,
        parser_fn=parse_api_response,
        parser_sources=list(API_PARSERS),
        chunk=chunk or settings.chunk_size,
        source=source,
    )
