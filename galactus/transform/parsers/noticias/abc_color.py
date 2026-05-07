from galactus.core.errors import ParserError
from galactus.core.records import HtmlSnapshot, ParsedRecord, RawRecord
from galactus.transform.base import Parser as BaseParser


class Parser(BaseParser):
    """Parses an HtmlSnapshot from abc.com.py into an Article entity."""

    def parse(self, record: RawRecord) -> ParsedRecord:
        if not isinstance(record, HtmlSnapshot):
            raise ParserError(f"abc_color parser expects HtmlSnapshot, got {type(record).__name__}")
        raise NotImplementedError
