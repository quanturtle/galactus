from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any

from sql.b_silver.product import Product


class ProductParser(ABC):
    """Field-extraction mixin for parsers producing Product silver rows.

    Mixed in alongside BaseParser: `class Parser(BaseParser, ProductParser): ...`.
    The mixin owns build_entity, which orchestrates the eight extract_*
    hooks into one Product. BaseParser.process_record drives iteration:
    it calls build_item() to split the decoded payload and passes each
    item to build_entity. Every Product field is optional, so the
    extract_* hooks return whatever they can find (or an empty value);
    build_entity does not filter.

    The extract_* methods are declared in the same order as Product's
    columns in sql/b_silver/product.py — concrete parsers implement them
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
    def extract_sku(self, item: Any) -> str | None: ...

    @abstractmethod
    def extract_name(self, item: Any) -> str: ...

    @abstractmethod
    def extract_brand(self, item: Any) -> str | None: ...

    @abstractmethod
    def extract_price(self, item: Any) -> Decimal | None: ...

    @abstractmethod
    def extract_currency(self, item: Any) -> str: ...

    @abstractmethod
    def extract_unit(self, item: Any) -> str | None: ...

    @abstractmethod
    def extract_image_urls(self, item: Any) -> list[str]: ...

    def build_entity(self, item: Any) -> Product:
        return Product(
            source=self.source,
            source_url=self.extract_source_url(item),
            sku=self.extract_sku(item),
            name=self.extract_name(item),
            brand=self.extract_brand(item),
            price=self.extract_price(item),
            currency=self.extract_currency(item),
            unit=self.extract_unit(item),
            image_urls=self.extract_image_urls(item),
        )
