"""Build gold.products by aggregating silver canonicals + price stats.

Names are normalized at the gold layer: measurement-unit suffixes are folded
to a consistent surface form ("x Kg", "x Un", "1 L", "500 ml", "200 g",
"500 cc", "x 4"), parenthetical SKU codes are stripped, and whitespace is
collapsed. Silver canonical_name stays as the LLM/SKU step produced it.
"""

import logging
import re

from galactus import db

logger = logging.getLogger(__name__)


# Patterns are applied in order. Each entry is (compiled_regex, replacement).
# Replacements may use backrefs.
_NORMALIZE_RULES: list[tuple[re.Pattern[str], str]] = [
    # Strip parenthetical SKU codes anywhere in the name.
    # All-digit codes (≥2 digits) — "(1167)X" -> "X", "X (249)" -> "X".
    # The 2-digit floor protects legitimate quantity hints like "(2)".
    (re.compile(r"\(\s*\d{2,}\s*\)"), ""),
    # Alphanumeric internal codes that mix at least one letter AND one digit
    # (e.g. "(CR03BO)", "(ME01BO)", "(8549)" already covered above).
    # The lookaheads guarantee letter+digit so we don't eat "(USA)" or "(UN)".
    (re.compile(
        r"\(\s*(?=[A-Z0-9]*[A-Z])(?=[A-Z0-9]*\d)[A-Z0-9]+\s*\)",
        re.IGNORECASE,
    ), ""),
    # Manufacturer reference tags: "REF:JDL209-08", "Ref. 50611", "REF#123".
    # The trailing code must contain at least one digit so we don't strip
    # words like "REFERENCIA" or "REF DE ALGO".
    (re.compile(
        r"\bref\.?[\s:#]+[\w\-]*\d[\w\-]*\b",
        re.IGNORECASE,
    ), ""),
    # Per-kilo phrases -> "x Kg"  (also handles "X KL" typo for KG).
    (re.compile(
        r"\b(?:por\s+kilos?|x\s*kilos?|x\s*kg\.?|x\s*kl\.?|por\s+kg)\b",
        re.IGNORECASE,
    ), "x Kg"),
    # Standalone parenthesised "(UN)" — strip word-boundary so the closing
    # paren can be followed by a non-word char (e.g. space).
    (re.compile(r"\(un\)", re.IGNORECASE), "x Un"),
    # "[por] <digits> unidad[es]" -> "x <digits> Un" — fires before the
    # standalone UN/UNDS suffix rule below because "unidades" contains "und".
    # Lookbehind for whitespace/start so the optional "por"/"x" prefix can be
    # absorbed without devouring the leading space and gluing "x" onto the
    # previous word.
    (re.compile(
        r"(?:(?<=\s)|^)(?:por|x)?\s*(\d+)\s*unidad(?:es)?\b",
        re.IGNORECASE,
    ), r"x \1 Un"),
    # "[por] <digits> UN" / "<digits> UNDS." / "<digits>UN" -> "x <digits> Un".
    (re.compile(
        r"(?:(?<=\s)|^)(?:por|x)?\s*(\d+)\s*und?s?\.?\b",
        re.IGNORECASE,
    ), r"x \1 Un"),
    # Per-unit phrases without preceding number -> "x Un".
    (re.compile(
        r"\b(?:por\s+unidad(?:es)?|x\s*unidad(?:es)?|x\s*un\.?)\b",
        re.IGNORECASE,
    ), "x Un"),
    # Package phrases -> "x Paq".
    (re.compile(
        r"\b(?:por\s+paquete|x\s*paq\.?)\b",
        re.IGNORECASE,
    ), "x Paq"),
    # Bunch phrases -> "x Mazo".
    (re.compile(r"\bx\s*mazo\.?\b", re.IGNORECASE), "x Mazo"),
    # Numeric weights/volumes — order matters: longest unit names first so
    # "ml" doesn't swallow part of "litros".
    # "1 litro" / "1.5 litros" / "1 lt" / "1 lts." / "1 lit." -> "1 L".
    (re.compile(
        r"(\d+(?:[.,]\d+)?)\s*(?:litros?|l\.?ts?\.?|lits?\.?)\b",
        re.IGNORECASE,
    ), r"\1 L"),
    # Standalone "L" with a number: "1L" / "1 L" / "1l" -> "1 L".
    (re.compile(
        r"(\d+(?:[.,]\d+)?)\s*l(?![a-záéíóú])",
        re.IGNORECASE,
    ), r"\1 L"),
    # Cubic centimeters: "200cc" / "200 c.c." / "200 CC" -> "200 cc".
    (re.compile(
        r"(\d+(?:[.,]\d+)?)\s*c\.?\s*c\.?\b",
        re.IGNORECASE,
    ), r"\1 cc"),
    # Milliliters: "500ml" / "500 ml" / "500 ML" -> "500 ml".
    (re.compile(r"(\d+(?:[.,]\d+)?)\s*ml\b", re.IGNORECASE), r"\1 ml"),
    # Kilograms with explicit number: "1kg" / "1 KG" / "1 kilos" -> "1 Kg".
    (re.compile(
        r"(\d+(?:[.,]\d+)?)\s*(?:kilos?|kg)\b",
        re.IGNORECASE,
    ), r"\1 Kg"),
    # Grams: "500g" / "500 GR" / "500 grs" / "500 gramos" -> "500 g".
    (re.compile(
        r"(\d+(?:[.,]\d+)?)\s*(?:gramos?|grs?\.?|g)\b",
        re.IGNORECASE,
    ), r"\1 g"),
    # Counts after "x": "X4" / "X 4" / "x 4 " -> "x 4". Case-insensitive so
    # "PILA AA X 4" lowercases to "PILA AA x 4".
    (re.compile(r"\bx\s*(\d+)\b", re.IGNORECASE), r"x \1"),
    # Standalone trailing unit suffix meaning "sold by the kilo":
    # "MORCILLA GUARANI KG." -> "MORCILLA GUARANI x Kg".
    # No equivalent end-anchored rule for "UN" — the digit-prefixed rules
    # above cover that case and an end-anchored UN rule would re-fire on
    # the already-emitted "x N Un".
    (re.compile(r"\s+kg\.?$", re.IGNORECASE), " x Kg"),
    (re.compile(r"\s+kl\.?$", re.IGNORECASE), " x Kg"),
]


def normalize_canonical_name(name: str) -> str:
    """Trim whitespace and standardize measurement-unit display in *name*."""
    if not name:
        return name
    s = name.strip()
    for pat, repl in _NORMALIZE_RULES:
        s = pat.sub(repl, s)
    # Some rewrites can leave a duplicated "x" (e.g. count rule sees "X 6"
    # and emits "x 6" while the unit rule has already prepended "x" before "6").
    s = re.sub(r"\b[xX]\s+x\s+", "x ", s)
    # Collapse runs of whitespace introduced by replacements / strip artifacts.
    s = " ".join(s.split())
    # Trim trailing punctuation left behind after stripping codes (e.g. "X.").
    s = s.rstrip(" .,;:-")
    return s


SELECT_AGGREGATES_SQL = """
SELECT
    c.id AS canonical_product_id,
    c.canonical_name,
    COALESCE(COUNT(DISTINCT p.source), 0)::int AS source_count,
    MIN(p.price) AS current_min_price,
    MAX(p.price) AS current_max_price,
    AVG(p.price) AS current_avg_price,
    MAX(p.updated_at) AS last_seen_at,
    COALESCE((
        SELECT COUNT(DISTINCT ph.price)::int
        FROM silver.price_history ph
        JOIN silver.canonical_product_members mm
          ON mm.silver_product_id = ph.silver_product_id
        WHERE mm.canonical_product_id = c.id
          AND ph.observed_at >= NOW() - INTERVAL '30 days'
    ), 0) AS price_changes_30d
FROM silver.canonical_products c
LEFT JOIN silver.canonical_product_members m ON m.canonical_product_id = c.id
LEFT JOIN silver.products p ON p.id = m.silver_product_id
GROUP BY c.id, c.canonical_name;
"""


PRUNE_SQL = """
DELETE FROM gold.products gp
WHERE NOT EXISTS (
    SELECT 1 FROM silver.canonical_products c WHERE c.id = gp.canonical_product_id
)
RETURNING canonical_product_id;
"""


_UPSERT_COLUMNS = (
    "canonical_name",
    "source_count",
    "current_min_price",
    "current_max_price",
    "current_avg_price",
    "last_seen_at",
    "price_changes_30d",
)


async def run() -> int:
    """Rebuild gold.products. Returns the number of rows upserted."""
    async with db.transaction() as tx:
        rows = await db.execute(SELECT_AGGREGATES_SQL, conn=tx)
        for r in rows:
            r["canonical_name"] = normalize_canonical_name(r["canonical_name"])

        if rows:
            await db.bulk_insert(
                "gold.products",
                rows,
                conflict_columns=("canonical_product_id",),
                update_columns=_UPSERT_COLUMNS,
                conn=tx,
            )
        # Touch updated_at on every row we just upserted (bulk_insert preserves
        # the existing default for non-conflict rows; conflict rows update it
        # via EXCLUDED.updated_at, but we didn't pass it — patch in one shot).
        await db.execute(
            "UPDATE gold.products SET updated_at = NOW()", conn=tx,
        )
        pruned = await db.execute(PRUNE_SQL, conn=tx)

    logger.info(
        "gold.products: upserted %d, pruned %d", len(rows), len(pruned),
    )
    return len(rows)
