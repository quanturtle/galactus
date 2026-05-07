from galactus.core.errors import ParserError
from galactus.core.records import HtmlSnapshot, ParsedRecord, RawRecord
from galactus.transform.base import Parser as BaseParser


class Parser(BaseParser):
    """Parses an HtmlSnapshot from ultimahora.com into an Article entity."""

    def parse(self, record: RawRecord) -> ParsedRecord:
        if not isinstance(record, HtmlSnapshot):
            raise ParserError(
                f"ultimahora parser expects HtmlSnapshot, got {type(record).__name__}"
            )
        # concrete extraction (json-ld + meta tags + body cleanup) ported from v1 later
        raise NotImplementedError
