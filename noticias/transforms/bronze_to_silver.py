"""Router that dispatches bronze-to-silver transforms by source type."""

import logging

from noticias.parsers import API_PARSERS, HTML_PARSERS
from noticias.transforms import api_responses, snapshots

logger = logging.getLogger(__name__)


async def run(source: str | None = None) -> int:
    if source is None:
        return await snapshots.run() + await api_responses.run()
    if source in HTML_PARSERS:
        return await snapshots.run(source)
    if source in API_PARSERS:
        return await api_responses.run(source)
    logger.warning("Unknown source %r — no parser registered", source)
    return 0
