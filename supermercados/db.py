import json

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from supermercados.settings import DATABASE_URL

pool = ConnectionPool(DATABASE_URL, min_size=2, max_size=10)


def get_conn():
    return pool.connection()


def bulk_insert(table: str, rows: list[dict]):
    """Insert rows into a table. Keys of the first row define columns."""
    if not rows:
        return
    columns = list(rows[0].keys())
    col_names = ", ".join(columns)
    placeholders = ", ".join(f"%({c})s" for c in columns)
    query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    prepared = []
    for row in rows:
        r = {}
        for k, v in row.items():
            if isinstance(v, (dict, list)):
                r[k] = json.dumps(v)
            else:
                r[k] = v
        prepared.append(r)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(query, prepared)
        conn.commit()


def execute(query: str, params=None):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params)
            if cur.description:
                return cur.fetchall()
        conn.commit()
    return []


def close():
    pool.close()
