import json
from typing import Any

from galactus.core.errors import ScraperError
from galactus.extract.base_scraper import BaseScraper
from galactus.infra.http import HttpRequest, HttpResponse
from sql.a_bronze.api_snapshots import ApiSnapshot

CLIENT_ID = "CADENA_REAL"
STORE_REFERENCE = "2"
PAGE_SIZE = 100

CATEGORY_TREE_QUERY = """query GetCategoryTree($getCategoryInput: GetCategoryInput!) {
  getCategory(getCategoryInput: $getCategoryInput) {
    reference
  }
}"""

PRODUCTS_QUERY = """query GetProductsByCategory($getProductsByCategoryInput: GetProductsByCategoryInput!) {
  getProductsByCategory(getProductsByCategoryInput: $getProductsByCategoryInput) {
    category {
      reference
      products {
        name
        price
        promotionPricePerSubUnit
        photosUrl
        unit
        sku
        ean
        brand
        stock
        slug
        description
        isActive
        isAvailable
      }
    }
    pagination {
      page
      pages
      total {
        value
      }
    }
  }
}"""


class Scraper(BaseScraper):
    """Scraper for realonline — Instaleap GraphQL catalog API, category-paginated into bronze.api_snapshots.

    realonline.com.py is an Instaleap headless storefront; its listing pages
    render client-side, so products can only be enumerated through the GraphQL
    API. The crawl is a three-level fan-out: the seed fetches the category tree,
    each top-level category reference fans out to its first product page, and
    each first page fans out to its remaining pages. Querying a top-level
    category rolls up every subcategory, so the 24 top-level references cover
    the whole catalog. The category-tree response is a discovery hop only and
    is kept out of bronze (see should_persist).
    """

    bronze_model = ApiSnapshot

    def build_url(
        self,
        operation: str | None = None,
        variables: dict[str, Any] | None = None,
        url: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> HttpRequest:
        # url=/params= path: seen_today re-hashes a captured request verbatim.
        if params is None:
            query = CATEGORY_TREE_QUERY if operation == "GetCategoryTree" else PRODUCTS_QUERY
            params = {
                "operationName": operation,
                "query": query,
                "variables": json.dumps(variables),
            }
        return HttpRequest(
            url=url if url is not None else self.config.base_url,
            headers=dict(self.config.headers),
            params=params,
        )

    def seed_urls(self) -> list[HttpRequest]:
        variables = {"getCategoryInput": {"clientId": CLIENT_ID, "storeReference": STORE_REFERENCE}}
        return [self.build_url("GetCategoryTree", variables)]

    def should_persist(self, request: HttpRequest) -> bool:
        # the category tree drives discovery only — it carries no catalog data.
        return request.params.get("operationName") == "GetProductsByCategory"

    def products_request(self, category_reference: str, page: int) -> HttpRequest:
        variables = {
            "getProductsByCategoryInput": {
                "clientId": CLIENT_ID,
                "storeReference": STORE_REFERENCE,
                "categoryReference": category_reference,
                "currentPage": page,
                "pageSize": PAGE_SIZE,
            }
        }
        return self.build_url("GetProductsByCategory", variables)

    def get_next_urls(self, response: HttpResponse, soup: object = None) -> list[HttpRequest]:
        body = response.json()
        data = body.get("data")
        if data is None:
            raise ScraperError(f"realonline: no data in {response.url}")

        # category tree -> page 1 of every top-level category
        if response.request.params.get("operationName") == "GetCategoryTree":
            categories = data.get("getCategory") or []
            return [
                self.products_request(category["reference"], 1)
                for category in categories
                if category.get("reference")
            ]

        # product page -> remaining pages, fanned out only from page 1
        page_input = json.loads(response.request.params["variables"])["getProductsByCategoryInput"]
        if page_input["currentPage"] != 1:
            return []
        result = data.get("getProductsByCategory") or {}
        pages = int((result.get("pagination") or {}).get("pages") or 1)
        return [
            self.products_request(page_input["categoryReference"], page)
            for page in range(2, pages + 1)
        ]
