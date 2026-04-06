from noticias.scrapers.abc_color import ABCColorScraper
from noticias.scrapers.adndigital import ADNDigitalScraper
from noticias.scrapers.cronica import CronicaScraper
from noticias.scrapers.elnacional import ElNacionalScraper
from noticias.scrapers.hoy import HoyScraper
from noticias.scrapers.lanacion import LaNacionScraper
from noticias.scrapers.latribuna import LaTribunaScraper
from noticias.scrapers.megacadena import MegacadenaScraper
from noticias.scrapers.npy import NPYScraper
from noticias.scrapers.ultimahora import UltimaHoraScraper

SCRAPERS = {
    "lanacion": LaNacionScraper,
    "abc": ABCColorScraper,
    "ultimahora": UltimaHoraScraper,
    "latribuna": LaTribunaScraper,
    "hoy": HoyScraper,
    "megacadena": MegacadenaScraper,
    "npy": NPYScraper,
    "adndigital": ADNDigitalScraper,
    "cronica": CronicaScraper,
    "elnacional": ElNacionalScraper,
}
