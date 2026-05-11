import asyncio
from collections.abc import Mapping
from typing import Any

import httpx

from galactus.core.errors import HttpError


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
    failures. URL construction (scheme, normalization) is the scraper's job, and
    so is fetch concurrency — BaseScraper.run caps how many requests are in flight,
    so this client carries no semaphore of its own (pool_size still bounds the
    underlying socket pool). All terminal failures surface as HttpError; the
    scraper re-raises them as ScraperError with source/URL context.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        follow_redirects: bool = True,
        retries: int = 3,
        retry_delay: float = 2.0,
        pool_size: int = 100,
    ) -> None:
        self.client = httpx.AsyncClient(
            timeout=timeout,
            headers=dict(headers) if headers else None,
            params=dict(params) if params else None,
            follow_redirects=follow_redirects,
            limits=httpx.Limits(
                max_connections=pool_size,
                max_keepalive_connections=pool_size,
            ),
        )
        self.retries = retries
        self.retry_delay = retry_delay

    async def get(
        self,
        url: str,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        last_exc: Exception | None = None
        last_response: httpx.Response | None = None

        # retry loop: pass through on <500, retry on 5xx and transient transport
        # failures (connect errors, timeouts, mid-stream server disconnects)
        for attempt in range(self.retries + 1):
            try:
                response = await self.client.get(url, headers=headers, params=params)
                last_response = response
                if response.status_code < 500:
                    return HttpResponse(response)
            except httpx.TransportError as exc:
                last_exc = exc

            if attempt < self.retries:
                await asyncio.sleep(self.retry_delay)

        # exhausted retries — surface as HttpError
        if last_response is not None:
            raise HttpError(
                f"GET {url} returned {last_response.status_code} after {self.retries + 1} attempts"
            )
        raise HttpError(f"GET {url} failed: {last_exc}") from last_exc

    async def __aenter__(self) -> "HttpClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.client.aclose()
        return
