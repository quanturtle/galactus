from galactus.core.records import ParsedRecord, RawRecord
from galactus.transform.base_parser import BaseParser


class Parser(BaseParser):
    """Parses HtmlSnapshots from ultimahora.com into Article entities."""

    def parse_batch(self, records: list[RawRecord]) -> list[ParsedRecord]:
        raise NotImplementedError
