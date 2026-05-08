from sql.a_bronze import RawRecord
from sql.b_silver import ParsedRecord
from galactus.transform.base_parser import BaseParser


class Parser(BaseParser):
    """Parses HtmlSnapshots from ultimahora.com into Article entities."""

    def parse_batch(self, records: list[RawRecord]) -> list[ParsedRecord]:
        raise NotImplementedError
