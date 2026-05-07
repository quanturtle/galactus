from galactus.core.errors import ParserError
from galactus.core.records import ApiSnapshot, ParsedRecord, RawRecord
from galactus.transform.base import Parser
from galactus.transform.registry import register_parser


@register_parser("stock")
class StockParser(Parser):
    """Parses an ApiSnapshot from stock.com.py into a Product entity."""

    def parse(self, record: RawRecord) -> ParsedRecord:
        if not isinstance(record, ApiSnapshot):
            raise ParserError(f"stock parser expects ApiSnapshot, got {type(record).__name__}")
        raise NotImplementedError
