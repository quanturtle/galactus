from collections.abc import Mapping
from typing import Any

import httpx

from galactus.config import HttpConfig


class HttpxResponse:
    """Adapter wrapping httpx.Response to satisfy core.HttpResponse Protocol."""

    def __init__(self, response: httpx.Response) -> None:
        self._response = response
        self.status_code = response.status_code
        self.headers = response.headers
        self.content = response.content
        self.text = response.text

    def json(self) -> Any:
        return self._response.json()


class HttpxClient:
    """Default HttpClient backed by httpx.AsyncClient."""

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
    ) -> HttpxResponse:
        response = await self._client.get(url, headers=headers, params=params)
        return HttpxResponse(response)

    async def aclose(self) -> None:
        await self._client.aclose()
        return


def make_http_client(config: HttpConfig) -> HttpxClient:
    """Construct an HttpxClient from an HttpConfig.

    HttpConfig is the right scoped slice for the constructor — this is the one
    allowed factory that takes a config sub-model rather than the whole
    PipelineConfig.
    """
    return HttpxClient(
        timeout=config.timeout_seconds,
        headers={"User-Agent": config.user_agent},
    )
