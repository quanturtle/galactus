import asyncio
import logging
from abc import ABC, abstractmethod
from pathlib import Path

import httpx
import yaml

logger = logging.getLogger(__name__)

DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_MAX_CONCURRENT = 5
DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_DELAY = 2
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)


class BaseScraper(ABC):
    """Base class for all scrapers."""

    source: str  # set by subclass

    def __init__(
        self,
        *,
        config_dir: Path | str | None = None,
        timeout: int = DEFAULT_REQUEST_TIMEOUT,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT,
        retry_attempts: int = DEFAULT_RETRY_ATTEMPTS,
        retry_delay: int = DEFAULT_RETRY_DELAY,
        user_agent: str = DEFAULT_USER_AGENT,
    ):
        if config_dir is not None:
            self.cfg = self._load_config(Path(config_dir), self.source)
        else:
            self.cfg = {}

        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers={"User-Agent": user_agent},
            follow_redirects=True,
        )
        self._retry_attempts = retry_attempts
        self._retry_delay = retry_delay
        self.semaphore = asyncio.Semaphore(max_concurrent)

    @staticmethod
    def _load_config(config_dir: Path, name: str) -> dict:
        path = config_dir / f"{name}.yml"
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return data.get("scraper") or {}

    async def close(self):
        await self.client.aclose()

    async def fetch(self, url: str, **kwargs) -> httpx.Response:
        """Fetch a URL with retry and concurrency limiting.

        Tries https first; if it fails with a transport error, falls back
        to http before exhausting retries.
        """
        async with self.semaphore:
            for attempt in range(1, self._retry_attempts + 1):
                try:
                    resp = await self.client.get(url, **kwargs)
                    resp.raise_for_status()
                    return resp
                except httpx.HTTPStatusError as e:
                    if e.response.status_code < 500:
                        raise
                    logger.warning(
                        "%s attempt %d/%d failed for %s: %s",
                        self.source, attempt, self._retry_attempts, url, e,
                    )
                    if attempt < self._retry_attempts:
                        await asyncio.sleep(self._retry_delay * attempt)
                    else:
                        raise
                except httpx.TransportError as e:
                    # Try http fallback before giving up
                    if url.startswith("https://"):
                        http_url = "http://" + url[len("https://"):]
                        try:
                            resp = await self.client.get(http_url, **kwargs)
                            resp.raise_for_status()
                            return resp
                        except (httpx.HTTPStatusError, httpx.TransportError):
                            pass  # fall through to normal retry/raise
                    logger.warning(
                        "%s attempt %d/%d failed for %s: %s",
                        self.source, attempt, self._retry_attempts, url, e,
                    )
                    if attempt < self._retry_attempts:
                        await asyncio.sleep(self._retry_delay * attempt)
                    else:
                        raise

    @abstractmethod
    async def scrape(self) -> None:
        """Scrape and store raw data."""
        ...

    async def run(self):
        """Lifecycle: scrape, then close."""
        try:
            logger.info("%s: starting scrape", self.source)
            await self.scrape()
            logger.info("%s: done", self.source)
        finally:
            await self.close()
