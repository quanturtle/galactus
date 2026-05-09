import asyncio
from collections.abc import Mapping
from typing import Any

import httpx

from galactus.core.errors import ScraperError


class HttpResponse:
    """Adapter exposing the fields scrapers read from an httpx.Response."""

    def __init__(self, response: httpx.Response) -> None:
        self._response = response
        self.status_code = response.status_code
        self.headers: dict[str, str] = dict(response.headers)

    @property
    def content(self) -> bytes:
        return self._response.content

    @property
    def text(self) -> str:
        return self._response.text

    def json(self) -> Any:
        return self._response.json()


class HttpClient:
    """HTTP client used by scrapers. Backed by httpx.AsyncClient internally.

    Concerns: opening connections, applying headers/params, retrying transient
    failures. URL construction (scheme, normalization) is the scraper's job.
    All terminal failures surface as ScraperError so callers can treat them
    uniformly.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        headers: Mapping[str, str] | None = None,
        follow_redirects: bool = True,
        retries: int = 3,
        retry_delay: float = 2.0,
        concurrency: int = 1,
        pool_size: int = 100,
    ) -> None:
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers=dict(headers) if headers else None,
            follow_redirects=follow_redirects,
            limits=httpx.Limits(
                max_connections=pool_size,
                max_keepalive_connections=pool_size,
            ),
        )
        self.retries = retries
        self.retry_delay = retry_delay
        self._semaphore = asyncio.Semaphore(concurrency)

    async def get(
        self,
        url: str,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        last_exc: Exception | None = None
        last_response: httpx.Response | None = None

        # retry loop: pass through on <500, retry on 5xx and transient errors
        for attempt in range(self.retries + 1):
            try:
                async with self._semaphore:
                    response = await self.client.get(url, headers=headers, params=params)
                last_response = response
                if response.status_code < 500:
                    return HttpResponse(response)
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                last_exc = exc

            if attempt < self.retries:
                await asyncio.sleep(self.retry_delay)

        # exhausted retries — surface as ScraperError
        if last_response is not None:
            raise ScraperError(
                f"GET {url} returned {last_response.status_code} after {self.retries + 1} attempts"
            )
        raise ScraperError(f"GET {url} failed: {last_exc}") from last_exc

    async def aclose(self) -> None:
        await self.client.aclose()
        return

    async def __aenter__(self) -> "HttpClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()
        return
