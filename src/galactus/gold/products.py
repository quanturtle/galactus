"""Maintain the gold star schema.

The star itself is defined in migrations:

- ``gold.dim_dates`` — table, seeded 2024-01-01..2030-12-31 by the migration.
- ``gold.dim_products``, ``gold.fact_prices`` — views over silver, no maintenance.

This module only extends ``dim_dates`` forward in time so the date key is
always available for incoming ``fact_prices`` rows.
"""

import logging

from galactus import db

logger = logging.getLogger(__name__)


_EXTEND_DIM_DATES_SQL = """
INSERT INTO gold.dim_dates (
    date_key, date, year, quarter, month, month_name,
    day, day_of_week, day_name, week_of_year, is_weekend
)
SELECT
    (TO_CHAR(d, 'YYYYMMDD'))::int,
    d::date,
    EXTRACT(YEAR FROM d)::smallint,
    EXTRACT(QUARTER FROM d)::smallint,
    EXTRACT(MONTH FROM d)::smallint,
    TO_CHAR(d, 'FMMonth'),
    EXTRACT(DAY FROM d)::smallint,
    EXTRACT(DOW FROM d)::smallint,
    TO_CHAR(d, 'FMDay'),
    EXTRACT(WEEK FROM d)::smallint,
    EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series(
    COALESCE((SELECT MAX(date) FROM gold.dim_dates), '2024-01-01'::date) + INTERVAL '1 day',
    MAKE_DATE(%(end_year)s, 12, 31),
    '1 day'
) d
ON CONFLICT (date_key) DO NOTHING
RETURNING date_key
"""


async def extend_dim_dates(end_year: int = 2030) -> int:
    """Idempotently ensure gold.dim_dates covers up to end_year-12-31.

    Returns the number of rows inserted (0 when already covered).
    """
    rows = await db.execute(_EXTEND_DIM_DATES_SQL, {"end_year": end_year})
    return len(rows)
