"""Step 2: fuzzy-name canonical match (pg_trgm).

For each silver.products row not yet linked to a canonical, find the best
existing silver.canonical_products row by trigram similarity. If similarity
clears the threshold, insert a membership row.

Does NOT create new canonical rows — only links to existing ones. Step 1
(sku_match) is responsible for seeding canonicals; this step rides on those.
"""

import logging

from galactus import db

logger = logging.getLogger(__name__)


SIMILARITY_THRESHOLD = 0.7


SQL = """
WITH unmatched AS (
    SELECT p.id, p.name
    FROM silver.products p
    LEFT JOIN silver.canonical_product_members m ON m.silver_product_id = p.id
    WHERE m.silver_product_id IS NULL
    {limit_clause}
),
matched AS (
    SELECT u.id AS product_id, c.id AS canonical_id
    FROM unmatched u
    CROSS JOIN LATERAL (
        SELECT id
        FROM silver.canonical_products c
        WHERE similarity(c.canonical_name, u.name) > %(threshold)s
        ORDER BY similarity(c.canonical_name, u.name) DESC
        LIMIT 1
    ) c
)
INSERT INTO silver.canonical_product_members
    (silver_product_id, canonical_product_id, match_method)
SELECT product_id, canonical_id, 'name_fuzzy' FROM matched
ON CONFLICT (silver_product_id) DO NOTHING
RETURNING silver_product_id;
"""


async def run(*, conn=None, limit: int | None = None) -> int:
    """Link unmatched products to canonicals via pg_trgm similarity."""
    limit_clause = ""
    if limit is not None:
        limit_clause = f"LIMIT {int(limit)}"
    sql = SQL.format(limit_clause=limit_clause)
    rows = await db.execute(sql, {"threshold": SIMILARITY_THRESHOLD}, conn=conn)
    count = len(rows)
    logger.info("name_match: linked %d products (threshold=%s)", count, SIMILARITY_THRESHOLD)
    return count
