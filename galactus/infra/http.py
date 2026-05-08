from collections.abc import Mapping
from typing import Any

import httpx


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
    """HTTP client used by scrapers. Backed by httpx.AsyncClient internally."""

    def __init__(
        self,
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
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> HttpResponse:
        response = await self._client.get(url, headers=headers, params=params)
        return HttpResponse(response)

    async def aclose(self) -> None:
        await self._client.aclose()
        return

    async def __aenter__(self) -> "HttpClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()
        return
