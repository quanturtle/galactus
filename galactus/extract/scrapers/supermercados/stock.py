from typing import Any

from galactus.extract.base_scraper import BaseScraper


class Scraper(BaseScraper):
    """Scraper for stock — same-domain BFS into bronze.html_snapshots."""

    def http_extras(self) -> dict[str, Any]:
        # stock.com.py serves TLS 1.2 with DH params that fail Python's default
        # SECLEVEL=2. Drop to SECLEVEL=1 for the handshake; cert validation stays on.
        return {"ssl_ciphers": "DEFAULT@SECLEVEL=1"}
