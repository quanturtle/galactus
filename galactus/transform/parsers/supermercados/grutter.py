from typing import Any

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.product import Product
from sql.base import Base


class Parser(BaseParser):
    """Parses ApiSnapshots from grutter.com.py into Product entities."""

    bronze_model = ApiSnapshot
    silver_model = Product

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        raise NotImplementedError
