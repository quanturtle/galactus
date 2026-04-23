"""Generic bronze→silver dispatch by source type."""

import logging
from typing import Awaitable, Callable, Mapping

logger = logging.getLogger(__name__)


async def run(
    *,
    html_parsers: Mapping[str, object],
    api_parsers: Mapping[str, object],
    snapshot_runner: Callable[..., Awaitable[int]],
    api_runner: Callable[..., Awaitable[int]],
    source: str | None = None,
) -> int:
    if source is None:
        return await snapshot_runner() + await api_runner()
    if source in html_parsers:
        return await snapshot_runner(source)
    if source in api_parsers:
        return await api_runner(source)
    logger.warning("Unknown source %r — no parser registered", source)
    return 0
