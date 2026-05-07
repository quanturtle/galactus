from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import httpx

from galactus.config import HttpConfig


class HttpResponse:
    """Adapter exposing the fields scrapers read from an httpx.Response."""

    def __init__(self, response: httpx.Response) -> None:
        self._response = response
        self.status_code = response.status_code
        self.headers = response.headers
        self.content = response.content
        self.text = response.text

    def json(self) -> Any:
        return self._response.json()


class HttpClient:
    """HTTP client used by scrapers. Backed by httpx.AsyncClient internally."""

    def __init__(
        self,
        *,
        timeout: float = 30.0,
        headers: Mapping[str, str] | None = None,
        follow_redirects: bool = True,
    ) -> None:
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers=dict(headers) if headers else None,
            follow_redirects=follow_redirects,
        )

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        response = await self._client.get(url, headers=headers, params=params)
        return HttpResponse(response)

    async def aclose(self) -> None:
        await self._client.aclose()
        return


@asynccontextmanager
async def open_http(config: HttpConfig) -> AsyncIterator[HttpClient]:
    """Open an HttpClient from config and close it on exit. Used per-source by stages."""
    client = HttpClient(
        timeout=config.timeout_seconds,
        headers={"User-Agent": config.user_agent},
    )
    try:
        yield client
    finally:
        await client.aclose()
