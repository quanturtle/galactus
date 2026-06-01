from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpClient, HttpClientBuilder


class Scraper(BaseScraper):
    """Scraper for stock — same-domain BFS into bronze.html_snapshots."""

    def make_http_client(self) -> HttpClient:
        # stock.com.py serves TLS 1.2 with DH params that fail Python's default
        # SECLEVEL=2. Drop to SECLEVEL=1 for the handshake; cert validation stays on.
        return (
            HttpClientBuilder()
            .set_timeout(self.config.timeout_seconds)
            .set_follow_redirects(self.config.follow_redirects)
            .set_pool_size(self.config.concurrency)
            .set_ssl_ciphers("DEFAULT@SECLEVEL=1")
            .build()
        )
