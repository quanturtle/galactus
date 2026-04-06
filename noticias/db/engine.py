"""Noticias DB initialisation — delegates to the shared async pool."""

from the_scraper.db import close_pool, init_pool

from noticias.config import settings


async def init() -> None:
    await init_pool(settings.database_url)


async def close() -> None:
    await close_pool()
