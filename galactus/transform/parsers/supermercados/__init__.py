from galactus.core.domain_registry import register_domain
from galactus.transform.parsers.supermercados.schema import Product

register_domain(
    name="supermercados",
    entity_model=Product,
    silver_table="silver.products",
    scraper_modules=(
        "galactus.extract.scrapers.supermercados.biggie",
        "galactus.extract.scrapers.supermercados.stock",
    ),
    parser_modules=(
        "galactus.transform.parsers.supermercados.biggie",
        "galactus.transform.parsers.supermercados.stock",
    ),
)
