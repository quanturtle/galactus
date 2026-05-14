from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from sql.base import Base


class ArticleParser(ABC):
    """Abstract field-extraction interface for parsers producing Article silver rows.

    Mixed in alongside BaseParser: `class Parser(BaseParser, ArticleParser): ...`.
    The methods are declared in the same order as Article's columns in
    sql/b_silver/article.py — concrete parsers implement them in that order
    so the file scans top-to-bottom against the silver schema.

    `item` is whatever per-entity payload the concrete parser's build_entities
    iterates over: a BeautifulSoup for HTML parsers, a dict for API parsers.
    `record` is the bronze row, exposed so source_url-class fields can be
    derived from it when the payload doesn't carry the URL.
    """

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
