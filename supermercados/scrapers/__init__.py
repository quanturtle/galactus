from supermercados.scrapers.arete import AreteScraper
from supermercados.scrapers.biggie import BiggieScraper
from supermercados.scrapers.casarica import CasaRicaScraper
from supermercados.scrapers.grutter import GrutterScraper
from supermercados.scrapers.superseis import SuperseisScraper

ALL_SCRAPERS = {
    "biggie": BiggieScraper,
    "superseis": SuperseisScraper,
    "casarica": CasaRicaScraper,
    "grutter": GrutterScraper,
    "arete": AreteScraper,
}
