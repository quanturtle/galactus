from galactus.core.domain_registry import register_domain
from galactus.transform.parsers.noticias.schema import Article

register_domain(
    name="noticias",
    entity_model=Article,
    silver_table="silver.articles",
    scraper_modules=(
        "galactus.extract.scrapers.noticias.abc_color",
        "galactus.extract.scrapers.noticias.ultimahora",
    ),
    parser_modules=(
        "galactus.transform.parsers.noticias.abc_color",
        "galactus.transform.parsers.noticias.ultimahora",
    ),
)
