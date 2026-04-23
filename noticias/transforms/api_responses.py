from galactus.transforms import api_responses as _generic
from noticias.article import Article
from noticias.config import settings
from noticias.parsers import API_PARSERS, parse_api_response


async def run(source: str | None = None, *, chunk: int | None = None) -> int:
    return await _generic.run(
        entity_cls=Article,
        parser_fn=parse_api_response,
        parser_sources=list(API_PARSERS),
        chunk=chunk or settings.chunk_size,
        source=source,
    )
