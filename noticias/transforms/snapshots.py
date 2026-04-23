from galactus.transforms import snapshots as _generic
from noticias.article import Article
from noticias.config import settings
from noticias.parsers import HTML_PARSERS, parse_snapshot


async def run(source: str | None = None, *, chunk: int | None = None) -> int:
    return await _generic.run(
        entity_cls=Article,
        parser_fn=parse_snapshot,
        parser_sources=list(HTML_PARSERS),
        chunk=chunk or settings.chunk_size,
        source=source,
    )
