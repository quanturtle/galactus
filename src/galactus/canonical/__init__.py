"""Canonicalization pipeline: silver.products -> silver.canonical_products.

Each step is an idempotent ``async def run(*, conn=None, limit=None) -> int``
returning the number of rows it touched. Steps are composable: run alone,
in sequence, or via the ``standardize`` CLI subcommand.

Order, cheap -> expensive:
    1. sku_match    — exact SKU groups
    2. name_match   — pg_trgm fuzzy match against existing canonicals
    3. llm_refine   — MLX merges / cleans canonical names
"""

from galactus.canonical import sku_match, name_match, llm_refine

__all__ = ["sku_match", "name_match", "llm_refine"]
