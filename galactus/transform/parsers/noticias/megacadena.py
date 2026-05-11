from typing import Any

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article
from sql.base import Base


class Parser(BaseParser):
    """Parses ApiSnapshots from megacadena.com.py into Article entities."""

    bronze_model = ApiSnapshot
    silver_model = Article

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        raise NotImplementedError
