from typing import Any

from bs4 import BeautifulSoup

from galactus.transform.base_parser import BaseParser
from galactus.transform.html_parser import decompress
from sql.a_bronze.html_snapshots import HtmlSnapshot
from sql.b_silver.article import Article
from sql.base import Base


class Parser(BaseParser):
    """Parses HtmlSnapshots from ultimahora.com into Article entities."""

    bronze_model = HtmlSnapshot
    silver_model = Article

    def decode(self, record: Base) -> BeautifulSoup:
        return self.html_parser.parse(decompress(record.html))

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        raise NotImplementedError
