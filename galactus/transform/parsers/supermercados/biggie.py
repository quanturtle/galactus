from galactus.core.errors import ParserError
from galactus.core.records import ApiSnapshot, ParsedRecord, RawRecord
from galactus.transform.base_parser import BaseParser


class Parser(BaseParser):
    """Parses an ApiSnapshot from biggie.com.py into a Product entity."""

    def run(self, record: RawRecord) -> ParsedRecord:
        if not isinstance(record, ApiSnapshot):
            raise ParserError(f"biggie parser expects ApiSnapshot, got {type(record).__name__}")
        raise NotImplementedError
