from galactus.transform.base_parser import BaseParser
from sql.base import Base


class Parser(BaseParser):
    """Parses ApiSnapshots from grutter.com.py into Product entities."""

    def parse_batch(self, records: list[Base]) -> list[Base]:
        raise NotImplementedError
