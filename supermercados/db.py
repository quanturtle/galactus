"""Supermercados DB initialisation — delegates to the shared async pool."""

from the_scraper.db import bulk_insert, close_pool, execute, init_pool

from supermercados.settings import DATABASE_URL


async def init() -> None:
    await init_pool(DATABASE_URL)


async def close() -> None:
    await close_pool()


__all__ = ["init", "close", "execute", "bulk_insert"]
