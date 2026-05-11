from typing import Any

from galactus.transform.base_parser import BaseParser
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.article import Article
from sql.base import Base


class Parser(BaseParser):
    """Parses HtmlSnapshots from elnacional.com.py into Article entities."""

    bronze_model = HtmlSnapshot
    silver_model = Article

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        raise NotImplementedError
