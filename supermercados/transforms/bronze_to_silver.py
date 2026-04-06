"""Router that dispatches bronze-to-silver transforms by source type."""

import logging

from supermercados.parsers import API_PARSERS, HTML_PARSERS
from supermercados.transforms import api_responses, snapshots

logger = logging.getLogger(__name__)

SNAPSHOT_SOURCES = set(HTML_PARSERS.keys())
API_SOURCES = set(API_PARSERS.keys())


def run(source: str | None = None):
    if source is None:
        snapshots.run()
        api_responses.run()
    elif source in SNAPSHOT_SOURCES:
        snapshots.run(source)
    elif source in API_SOURCES:
        api_responses.run(source)
    else:
        logger.warning("Unknown source %r — no parser registered", source)
