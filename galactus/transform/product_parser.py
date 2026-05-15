import re
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

    # unit-extraction patterns evaluated in order; first match wins. cover the
    # inline unit info embedded in ~80% of product names across the six
    # supermercado sources. ordering puts kg ahead of g (so "X KG" wins on
    # bulk-meat names) and l ahead of ml (so "1.8 LT" wins on detergents); cc
    # is the rare per-oil case.
    UNIT_PATTERNS: list[tuple[re.Pattern[str], str]] = [
        (re.compile(r"\b(?:x\s*kg|kilos?|\d+\s*kg)\b\.?", re.IGNORECASE), "kg"),
        (re.compile(r"\b\d+(?:[.,]\d+)?\s*lts?\b\.?", re.IGNORECASE), "l"),
        (re.compile(r"\b\d+(?:[.,]\d+)?\s*l\b\.?", re.IGNORECASE), "l"),
        (re.compile(r"\b\d+(?:[.,]\d{1,3})?\s*ml\b\.?", re.IGNORECASE), "ml"),
        (re.compile(r"\b\d+\s*(?:gramos?|gr?s?)\b\.?", re.IGNORECASE), "g"),
        (re.compile(r"\b\d+\s*cc\b\.?", re.IGNORECASE), "cc"),
    ]

    # extract a normalized unit symbol (kg | l | ml | g | cc) from a product
    # name; None when no pattern matches (bundles, counts, name-only items).
    # subclasses delegate extract_unit to this when no structured field is
    # available on the source.
    def parse_unit_from_name(self, name: str) -> str | None:
        for pattern, unit in self.UNIT_PATTERNS:
            if pattern.search(name):
                return unit
        return None

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
