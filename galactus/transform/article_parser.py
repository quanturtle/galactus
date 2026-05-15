from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from sql.b_silver.article import Article


class ArticleParser(ABC):
    """Field-extraction mixin for parsers producing Article silver rows.

    Mixed in alongside BaseParser: `class Parser(BaseParser, ArticleParser): ...`.
    The mixin owns build_entity, which orchestrates the eight extract_*
    hooks into one Article. BaseParser.process_record drives iteration:
    it calls build_item() to split the decoded payload and passes each
    item to build_entity. Every Article field is optional, so the
    extract_* hooks return whatever they can find (or an empty value);
    build_entity does not filter.

    The extract_* methods are declared in the same order as Article's
    columns in sql/b_silver/article.py — concrete parsers implement them
    in that order so the file scans top-to-bottom against the silver
    schema.

    `item` is whatever per-entity payload build_item yields — typically
    a dict that bundles the per-entity slice of the decoded payload
    together with any bronze-derived context the extract_* methods need
    (e.g., source_url for HTML single-page parsers whose payload doesn't
    carry the URL).
    """

    source: str

    @abstractmethod
    def extract_source_url(self, item: Any) -> str: ...

    @abstractmethod
    def extract_title(self, item: Any) -> str: ...

    @abstractmethod
    def extract_body(self, item: Any) -> str | None: ...

    @abstractmethod
    def extract_authors(self, item: Any) -> list[str]: ...

    @abstractmethod
    def extract_published_at(self, item: Any) -> datetime | None: ...

    @abstractmethod
    def extract_section(self, item: Any) -> str | None: ...

    @abstractmethod
    def extract_tags(self, item: Any) -> list[str]: ...

    @abstractmethod
    def extract_image_urls(self, item: Any) -> list[str]: ...

    def build_entity(self, item: Any) -> Article:
        return Article(
            source=self.source,
            source_url=self.extract_source_url(item),
            title=self.extract_title(item),
            body=self.extract_body(item),
            authors=self.extract_authors(item),
            published_at=self.extract_published_at(item),
            section=self.extract_section(item),
            tags=self.extract_tags(item),
            image_urls=self.extract_image_urls(item),
        )
