from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from sql.b_silver.article import Article
from sql.base import Base


class ArticleParser(ABC):
    """Field-extraction mixin for parsers producing Article silver rows.

    Mixed in alongside BaseParser: `class Parser(BaseParser, ArticleParser): ...`.
    The mixin owns build_entity, which orchestrates the eight extract_*
    hooks into one Article. Concrete parsers retain build_entities
    (iteration + required-field guards) and delegate per-item construction
    to self.build_entity(item, record).

    The extract_* methods are declared in the same order as Article's
    columns in sql/b_silver/article.py — concrete parsers implement them
    in that order so the file scans top-to-bottom against the silver
    schema.

    `item` is whatever per-entity payload the concrete parser's
    build_entities iterates over: a BeautifulSoup for HTML parsers, a
    dict for API parsers. `record` is the bronze row, exposed so
    source_url-class fields can be derived from it when the payload
    doesn't carry the URL.
    """

    source: str

    @abstractmethod
    def extract_source_url(self, item: Any, record: Base) -> str: ...

    @abstractmethod
    def extract_title(self, item: Any, record: Base) -> str: ...

    @abstractmethod
    def extract_body(self, item: Any, record: Base) -> str | None: ...

    @abstractmethod
    def extract_authors(self, item: Any, record: Base) -> list[str]: ...

    @abstractmethod
    def extract_published_at(self, item: Any, record: Base) -> datetime | None: ...

    @abstractmethod
    def extract_section(self, item: Any, record: Base) -> str | None: ...

    @abstractmethod
    def extract_tags(self, item: Any, record: Base) -> list[str]: ...

    @abstractmethod
    def extract_image_urls(self, item: Any, record: Base) -> list[str]: ...

    def build_entity(self, item: Any, record: Base) -> Article:
        return Article(
            source=self.source,
            source_url=self.extract_source_url(item, record),
            title=self.extract_title(item, record),
            body=self.extract_body(item, record),
            authors=self.extract_authors(item, record),
            published_at=self.extract_published_at(item, record),
            section=self.extract_section(item, record),
            tags=self.extract_tags(item, record),
            image_urls=self.extract_image_urls(item, record),
        )
