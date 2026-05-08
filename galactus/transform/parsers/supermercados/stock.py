from sqlmodel import SQLModel
from galactus.transform.base_parser import BaseParser


class Parser(BaseParser):
    """Parses ApiSnapshots from stock.com.py into Product entities."""

    def parse_batch(self, records: list[SQLModel]) -> list[SQLModel]:
        raise NotImplementedError
