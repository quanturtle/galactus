"""Domain composition root.

A domain is a dict of per-source Pipelines: each pipeline bundles a scraper
class, a parser (HTML preservation policy), a transformer (bronze→silver
extractor), and a runner that knows how to execute the transform step for
that source. `make_domain(...)` iterates the domain's sources and calls
`make_pipeline(...)` for each. An optional `image_config` wires a single
shared `ImageScraper` instance onto every Pipeline.
"""

from dataclasses import dataclass, field
from types import ModuleType
from typing import Awaitable, Callable

from galactus.parsers import Parser
from galactus.scrapers.images import ImageConfig, ImageScraper
from galactus.transformers import api_responses, snapshots


@dataclass
class Pipeline:
    """End-to-end bundle for a single source: scraper + parser + transformer."""

    source: str
    scraper: type
    parser: Parser
    transformer: Callable
    transformer_kind: str  # "html" | "api"
    _transform_runner: Callable[..., Awaitable[int]] = field(repr=False)
    _image_scraper: ImageScraper | None = field(default=None, repr=False)

    async def scrape(self) -> None:
        await self.scraper().run()

    async def transform(self) -> int:
        return await self._transform_runner(self.source)

    async def download_images(self) -> int:
        if self._image_scraper is None:
            return 0
        return await self._image_scraper.run(source=self.source)

    async def run(self) -> None:
        """Scrape + transform for this source. Image download stays separate."""
        await self.scrape()
        await self.transform()


def make_pipeline(
    *,
    source: str,
    scraper: type,
    parser: Parser,
    transformer: Callable,
    transformer_kind: str,
    transform_runner: Callable[..., Awaitable[int]],
    image_scraper: ImageScraper | None = None,
) -> Pipeline:
    return Pipeline(
        source=source,
        scraper=scraper,
        parser=parser,
        transformer=transformer,
        transformer_kind=transformer_kind,
        _transform_runner=transform_runner,
        _image_scraper=image_scraper,
    )


@dataclass
class DomainSpec:
    """Everything the CLI needs to run a domain's scrape / transform / image ops."""

    name: str
    description: str
    pipelines: dict[str, Pipeline]
    image_scraper: ImageScraper | None = None
    setup: Callable[[], None] | None = None


def make_domain(
    *,
    name: str,
    description: str,
    entity_cls: type,
    scrapers: ModuleType,
    transformers: ModuleType,
    chunk_size: int,
    image_config: ImageConfig | None = None,
    setup: Callable[[], None] | None = None,
) -> DomainSpec:
    html = transformers.HTML_TRANSFORMERS
    api = transformers.API_TRANSFORMERS
    parser_registry = scrapers.PARSER_REGISTRY

    async def _snapshot_runner(source: str | None = None) -> int:
        return await snapshots.run(
            entity_cls=entity_cls,
            transformer_fn=transformers.transform_snapshot,
            transformer_sources=list(html),
            chunk=chunk_size,
            source=source,
        )

    async def _api_runner(source: str | None = None) -> int:
        return await api_responses.run(
            entity_cls=entity_cls,
            transformer_fn=transformers.transform_api_response,
            transformer_sources=list(api),
            chunk=chunk_size,
            source=source,
        )

    image_scraper: ImageScraper | None = None
    if image_config is not None:
        image_scraper = ImageScraper(
            config=image_config,
            html_transformer_fn=transformers.transform_snapshot,
            api_transformer_fn=transformers.transform_api_response,
            html_sources=list(html),
            api_sources=list(api),
        )

    pipelines: dict[str, Pipeline] = {}
    for source, scraper_cls in scrapers.SCRAPERS.items():
        if source in html:
            kind, transformer_fn, runner = "html", html[source], _snapshot_runner
        elif source in api:
            kind, transformer_fn, runner = "api", api[source], _api_runner
        else:
            raise ValueError(
                f"Source {source!r} has a scraper but no transformer registered"
            )

        pipelines[source] = make_pipeline(
            source=source,
            scraper=scraper_cls,
            parser=parser_registry.get(source),
            transformer=transformer_fn,
            transformer_kind=kind,
            transform_runner=runner,
            image_scraper=image_scraper,
        )

    return DomainSpec(
        name=name,
        description=description,
        pipelines=pipelines,
        image_scraper=image_scraper,
        setup=setup,
    )
