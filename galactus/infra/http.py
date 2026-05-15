import asyncio
import ssl
from collections.abc import Mapping
from typing import Any

import httpx

from galactus.core.errors import HttpError


class HttpRequest:
    """Adapter for the outbound request — symmetric to HttpResponse.

    Frozen-ish: dict inputs are normalized into sorted tuples internally so
    instances are hashable. The `headers` and `params` properties return fresh
    dicts on each access, so callers cannot mutate the stored state.
    """

    __slots__ = ("url", "_headers", "_params")

    def __init__(
        self,
        url: str,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, str] | None = None,
    ) -> None:
        self.url = url
        self._headers: tuple[tuple[str, str], ...] = tuple(sorted((headers or {}).items()))
        self._params: tuple[tuple[str, str], ...] = tuple(sorted((params or {}).items()))

    @property
    def headers(self) -> dict[str, str]:
        return dict(self._headers)

    @property
    def params(self) -> dict[str, str]:
        return dict(self._params)

    def __hash__(self) -> int:
        return hash((self.url, self._headers, self._params))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, HttpRequest):
            return NotImplemented
        return (self.url, self._headers, self._params) == (
            other.url,
            other._headers,
            other._params,
        )

    def __repr__(self) -> str:
        return f"HttpRequest(url={self.url!r}, headers={self.headers!r}, params={self.params!r})"


class HttpResponse:
    """Adapter exposing the fields scrapers read from an httpx.Response."""

    __slots__ = ("_response", "request", "status_code", "headers")

    def __init__(self, response: httpx.Response, request: HttpRequest) -> None:
        self._response = response
        self.request = request
        self.status_code = response.status_code
        self.headers: dict[str, str] = dict(response.headers)

    @property
    def url(self) -> str:
        return str(self._response.url)

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

    Concerns: opening connections and retrying transient failures. Per-request
    headers and params arrive on the HttpRequest, not on the client. URL
    construction (scheme, normalization) is the scraper's job, and so is fetch
    concurrency — BaseScraper.run caps how many requests are in flight, so this
    client carries no semaphore of its own (pool_size still bounds the underlying
    socket pool). All terminal failures surface as HttpError; the scraper
    re-raises them as ScraperError with source/URL context.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        follow_redirects: bool = True,
        retries: int = 3,
        retry_delay: float = 2.0,
        pool_size: int = 100,
        **httpx_kwargs: Any,
    ) -> None:
        if "ssl_ciphers" in httpx_kwargs:
            ctx = ssl.create_default_context()
            ctx.set_ciphers(httpx_kwargs.pop("ssl_ciphers"))
            httpx_kwargs["verify"] = ctx
        self.client = httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=follow_redirects,
            limits=httpx.Limits(
                max_connections=pool_size,
                max_keepalive_connections=pool_size,
            ),
            **httpx_kwargs,
        )
        self.retries = retries
        self.retry_delay = retry_delay

    async def get(self, request: HttpRequest) -> HttpResponse:
        last_exc: Exception | None = None
        last_response: httpx.Response | None = None

        # retry loop: pass through on <500, retry on 5xx and transient transport
        # failures (connect errors, timeouts, mid-stream server disconnects)
        for attempt in range(self.retries + 1):
            try:
                response = await self.client.get(
                    request.url,
                    headers=request.headers,
                    params=request.params,
                )
                last_response = response
                if response.status_code < 500:
                    return HttpResponse(response, request)
            except httpx.TransportError as exc:
                last_exc = exc

            if attempt < self.retries:
                await asyncio.sleep(self.retry_delay)

        # exhausted retries — surface as HttpError
        if last_response is not None:
            raise HttpError(
                f"GET {request.url} returned {last_response.status_code} after {self.retries + 1} attempts"
            )
        raise HttpError(f"GET {request.url} failed: {last_exc}") from last_exc

    async def __aenter__(self) -> "HttpClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.client.aclose()
        return
