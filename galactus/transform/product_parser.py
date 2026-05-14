from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any

from sql.base import Base


class ProductParser(ABC):
    """Abstract field-extraction interface for parsers producing Product silver rows.

    Mixed in alongside BaseParser: `class Parser(BaseParser, ProductParser): ...`.
    The methods are declared in the same order as Product's columns in
    sql/b_silver/product.py — concrete parsers implement them in that order
    so the file scans top-to-bottom against the silver schema.

    `item` is whatever per-entity payload the concrete parser's build_entities
    iterates over: a BeautifulSoup for HTML parsers, a dict for API parsers.
    `record` is the bronze row, exposed so source_url-class fields can be
    derived from it when the payload doesn't carry the URL.
    """

    @abstractmethod
    def extract_source_url(self, item: Any, record: Base) -> str: ...

    @abstractmethod
    def extract_sku(self, item: Any, record: Base) -> str | None: ...

    @abstractmethod
    def extract_name(self, item: Any, record: Base) -> str: ...

    @abstractmethod
    def extract_brand(self, item: Any, record: Base) -> str | None: ...

    @abstractmethod
    def extract_price(self, item: Any, record: Base) -> Decimal | None: ...

    @abstractmethod
    def extract_currency(self, item: Any, record: Base) -> str: ...

    @abstractmethod
    def extract_unit(self, item: Any, record: Base) -> str | None: ...

    @abstractmethod
    def extract_image_urls(self, item: Any, record: Base) -> list[str]: ...
