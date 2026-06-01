import logging
import ssl
from collections.abc import Mapping
from types import TracebackType
from typing import Any

import httpx

logger = logging.getLogger(__name__)


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
    def params(self) -> dict[str, str] | None:
        """Returns None when no params were set so httpx preserves any query string baked into self.url; returns a dict copy otherwise. API scrapers that read params back (get_next_urls) only call this on requests they constructed with non-empty params and so always see a dict."""
        return dict(self._params) if self._params else None

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


class HttpRequestBuilder:
    """Fluent builder for HttpRequest — layer the url, headers, and params in any
    order, then build() freezes them into an HttpRequest. set_headers/set_params
    merge into what's already staged, so a base set can be extended per request.
    """

    __slots__ = ("_url", "_headers", "_params")

    def __init__(self) -> None:
        self._url: str = ""
        self._headers: dict[str, str] = {}
        self._params: dict[str, str] = {}

    def set_url(self, url: str) -> "HttpRequestBuilder":
        self._url = url
        return self

    def set_headers(self, headers: Mapping[str, str]) -> "HttpRequestBuilder":
        self._headers.update(headers)
        return self

    def set_params(self, params: Mapping[str, str]) -> "HttpRequestBuilder":
        self._params.update(params)
        return self

    def build(self) -> "HttpRequest":
        return HttpRequest(self._url, self._headers, self._params)


class HttpResponse:
    """Adapter exposing the fields scrapers read from an httpx.Response."""

    __slots__ = ("response", "request", "status_code")

    def __init__(self, response: httpx.Response, request: HttpRequest) -> None:
        self.response = response
        self.request = request
        self.status_code = response.status_code

    @property
    def url(self) -> str:
        return str(self.response.url)

    @property
    def content(self) -> bytes:
        return self.response.content

    @property
    def text(self) -> str:
        return self.response.text

    @property
    def headers(self) -> httpx.Headers:
        return self.response.headers

    def json(self) -> Any:
        return self.response.json()


class HttpClient:
    """HTTP client used by scrapers. Backed by httpx.AsyncClient internally.

    Concerns: opening connections. Per-request headers and params arrive on the
    HttpRequest, not on the client. URL construction (scheme, normalization) is
    the scraper's job, and so is fetch concurrency — BaseScraper.run caps how
    many requests are in flight, so this client carries no semaphore of its own
    (pool_size still bounds the underlying socket pool). Single-attempt: every
    outcome — transport error (status 0), 4xx, 5xx, 2xx — is returned as an
    HttpResponse; the scraper routes failures to bronze.failed_snapshots.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        follow_redirects: bool = True,
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
        logger.info(
            "HttpClient initialized (timeout=%s, pool_size=%s)",
            timeout,
            pool_size,
        )

    async def get(self, request: HttpRequest) -> HttpResponse:
        logger.info("GET %s", request.url)
        try:
            response = await self.client.get(
                request.url,
                headers=request.headers,
                params=request.params,
            )
        except httpx.TransportError as exc:
            logger.warning("GET %s failed: %s", request.url, exc)
            return HttpResponse(httpx.Response(status_code=0, text=str(exc)), request)
        log = logger.info if 200 <= response.status_code < 300 else logger.warning
        log("GET %s -> %s", request.url, response.status_code)
        return HttpResponse(response, request)

    async def __aenter__(self) -> "HttpClient":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.client.aclose()
        return


class HttpClientBuilder:
    """Fluent builder for HttpClient — set transport knobs piecewise, then build()
    opens the client. Defaults mirror HttpClient's own, so only the knobs a given
    pipeline cares about need to be set.
    """

    __slots__ = ("_timeout", "_follow_redirects", "_pool_size", "_ssl_ciphers")

    def __init__(self) -> None:
        self._timeout: float = 30.0
        self._follow_redirects: bool = True
        self._pool_size: int = 100
        self._ssl_ciphers: str | None = None

    def set_timeout(self, seconds: float) -> "HttpClientBuilder":
        self._timeout = seconds
        return self

    def set_follow_redirects(self, enabled: bool) -> "HttpClientBuilder":
        self._follow_redirects = enabled
        return self

    def set_pool_size(self, size: int) -> "HttpClientBuilder":
        self._pool_size = size
        return self

    def set_ssl_ciphers(self, ciphers: str) -> "HttpClientBuilder":
        self._ssl_ciphers = ciphers
        return self

    def build(self) -> HttpClient:
        extras = {} if self._ssl_ciphers is None else {"ssl_ciphers": self._ssl_ciphers}
        return HttpClient(
            timeout=self._timeout,
            follow_redirects=self._follow_redirects,
            pool_size=self._pool_size,
            **extras,
        )
