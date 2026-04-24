"""Generic bronze→silver dispatch by source type."""

import logging
from typing import Awaitable, Callable, Mapping

logger = logging.getLogger(__name__)


async def run(
    *,
    html_transformers: Mapping[str, object],
    api_transformers: Mapping[str, object],
    snapshot_runner: Callable[..., Awaitable[int]],
    api_runner: Callable[..., Awaitable[int]],
    source: str | None = None,
) -> int:
    if source is None:
        return await snapshot_runner() + await api_runner()
    if source in html_transformers:
        return await snapshot_runner(source)
    if source in api_transformers:
        return await api_runner(source)
    logger.warning("Unknown source %r — no transformer registered", source)
    return 0
