"""Router that dispatches bronze-to-silver transforms by source type."""

import logging

from supermercados.parsers import API_PARSERS, HTML_PARSERS
from supermercados.transforms import api_responses, snapshots

logger = logging.getLogger(__name__)


async def run(source: str | None = None):
    if source is None:
        await snapshots.run()
        await api_responses.run()
    elif source in HTML_PARSERS:
        await snapshots.run(source)
    elif source in API_PARSERS:
        await api_responses.run(source)
    else:
        logger.warning("Unknown source %r — no parser registered", source)
