from galactus.core.errors import ParserError
from galactus.core.records import ApiSnapshot, ParsedRecord, RawRecord
from galactus.transform.base import Parser as BaseParser


class Parser(BaseParser):
    """Parses an ApiSnapshot from biggie.com.py into a Product entity."""

    def parse(self, record: RawRecord) -> ParsedRecord:
        if not isinstance(record, ApiSnapshot):
            raise ParserError(f"biggie parser expects ApiSnapshot, got {type(record).__name__}")
        raise NotImplementedError
