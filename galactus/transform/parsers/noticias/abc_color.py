import json
from typing import Any

from galactus.transform.base_parser import BaseParser
from galactus.transform.html_parser import decompress
from sql.a_bronze.api_snapshots import ApiSnapshot
from sql.b_silver.article import Article
from sql.base import Base


class Parser(BaseParser):
    """Parses ApiSnapshots from abc.com.py into Article entities."""

    bronze_model = ApiSnapshot
    silver_model = Article

    def decode(self, record: Base) -> dict:
        return json.loads(decompress(record.body))

    def build_entities(self, record: Base, decoded: Any) -> list[Base]:
        raise NotImplementedError
