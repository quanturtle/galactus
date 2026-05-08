from sqlmodel import SQLModel
from galactus.transform.base_parser import BaseParser


class Parser(BaseParser):
    """Parses HtmlSnapshots from npy.com.py into Article entities."""

    def parse_batch(self, records: list[SQLModel]) -> list[SQLModel]:
        raise NotImplementedError
