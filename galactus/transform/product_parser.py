from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any

from sql.b_silver.product import Product
from sql.base import Base


class ProductParser(ABC):
    """Field-extraction mixin for parsers producing Product silver rows.

    Mixed in alongside BaseParser: `class Parser(BaseParser, ProductParser): ...`.
    The mixin owns build_entity, which orchestrates the eight extract_*
    hooks into one Product. Concrete parsers retain build_entities
    (iteration + required-field guards) and delegate per-item construction
    to self.build_entity(item, record).

    The extract_* methods are declared in the same order as Product's
    columns in sql/b_silver/product.py — concrete parsers implement them
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

    def build_entity(self, item: Any, record: Base) -> Product:
        return Product(
            source=self.source,
            source_url=self.extract_source_url(item, record),
            sku=self.extract_sku(item, record),
            name=self.extract_name(item, record),
            brand=self.extract_brand(item, record),
            price=self.extract_price(item, record),
            currency=self.extract_currency(item, record),
            unit=self.extract_unit(item, record),
            image_urls=self.extract_image_urls(item, record),
        )
