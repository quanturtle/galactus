from sql.base import Base
from galactus.transform.base_parser import BaseParser


class Parser(BaseParser):
    """Parses HtmlSnapshots from latribuna.com.py into Article entities."""

    def parse_batch(self, records: list[Base]) -> list[Base]:
        raise NotImplementedError
