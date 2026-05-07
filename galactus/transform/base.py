from abc import ABC, abstractmethod
from typing import Any

from galactus.core.records import ParsedRecord, RawRecord
from galactus.core.types import SourceName


class Parser(ABC):
    """Strategy contract for converting one source's RawRecord into a ParsedRecord.

    Parsers are pure: they receive a RawRecord and return a ParsedRecord whose
    `entity` is a domain pydantic model. They never read or write the database.
    """

    def __init__(
        self,
        source: SourceName,
        options: dict[str, Any],
    ) -> None:
        self.source = source
        self.options = options

    @abstractmethod
    def parse(self, record: RawRecord) -> ParsedRecord: ...
