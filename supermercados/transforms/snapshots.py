from galactus.transforms import snapshots as _generic
from supermercados.config import settings
from supermercados.parsers import HTML_PARSERS, parse_snapshot
from supermercados.product import Product


async def run(source: str | None = None, *, chunk: int | None = None) -> int:
    return await _generic.run(
        entity_cls=Product,
        parser_fn=parse_snapshot,
        parser_sources=list(HTML_PARSERS),
        chunk=chunk or settings.chunk_size,
        source=source,
    )
