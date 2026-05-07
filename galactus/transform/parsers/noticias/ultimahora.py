from galactus.core.errors import ParserError
from galactus.core.records import HtmlSnapshot, ParsedRecord, RawRecord
from galactus.transform.base import Parser
from galactus.transform.registry import PARSERS


@PARSERS.register("ultimahora")
class UltimaHoraParser(Parser):
    """Parses an HtmlSnapshot from ultimahora.com into an Article entity."""

    def parse(self, record: RawRecord) -> ParsedRecord:
        if not isinstance(record, HtmlSnapshot):
            raise ParserError(
                f"ultimahora parser expects HtmlSnapshot, got {type(record).__name__}"
            )
        # concrete extraction (json-ld + meta tags + body cleanup) ported from v1 later
        raise NotImplementedError
