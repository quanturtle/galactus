from galactus.transform.base_parser import BaseParser
from sql.base import Base


class Parser(BaseParser):
    """Parses HtmlSnapshots from latribuna.com.py into Article entities."""

    def parse_batch(self, records: list[Base]) -> list[Base]:
        raise NotImplementedError
