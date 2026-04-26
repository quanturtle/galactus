"""Step 1: exact-SKU canonical grouping.

For each distinct non-empty SKU in silver.products that does not yet have a
membership row, create a silver.canonical_products row (heuristic name = the
longest product name in the group) and link every product with that SKU via
silver.canonical_product_members.

Cross-source SKU collision risk: source-specific codes can collide. Acceptable
for v1; Step 3 (LLM) can flag bad merges and a future tool can split them.
"""

import logging

from galactus import db

logger = logging.getLogger(__name__)


SQL = """
WITH sku_groups AS (
    SELECT
        p.sku,
        (array_agg(p.name ORDER BY length(p.name) DESC, p.id))[1] AS heuristic_name,
        array_agg(p.id) AS product_ids
    FROM silver.products p
    LEFT JOIN silver.canonical_product_members m ON m.silver_product_id = p.id
    WHERE p.sku IS NOT NULL
      AND length(p.sku) > 0
      AND m.silver_product_id IS NULL
    GROUP BY p.sku
),
new_canonicals AS (
    INSERT INTO silver.canonical_products (canonical_name)
    SELECT heuristic_name FROM sku_groups
    RETURNING id, canonical_name
),
-- Pair each new_canonical row with one sku_group via row_number() to handle
-- duplicate heuristic_name across groups deterministically.
numbered_groups AS (
    SELECT sg.*, row_number() OVER (PARTITION BY heuristic_name ORDER BY sku) AS rn
    FROM sku_groups sg
),
numbered_canonicals AS (
    SELECT nc.*, row_number() OVER (PARTITION BY canonical_name ORDER BY id) AS rn
    FROM new_canonicals nc
),
linked AS (
    SELECT nc.id AS canonical_id, unnest(ng.product_ids) AS product_id
    FROM numbered_groups ng
    JOIN numbered_canonicals nc
      ON nc.canonical_name = ng.heuristic_name AND nc.rn = ng.rn
)
INSERT INTO silver.canonical_product_members
    (silver_product_id, canonical_product_id, match_method)
SELECT product_id, canonical_id, 'sku' FROM linked
ON CONFLICT (silver_product_id) DO NOTHING
RETURNING silver_product_id;
"""


async def run(*, conn=None, limit: int | None = None) -> int:
    """Link silver.products by exact SKU. ``limit`` is ignored — runs in one pass."""
    rows = await db.execute(SQL, conn=conn)
    count = len(rows)
    logger.info("sku_match: linked %d products", count)
    return count
