import datetime
from typing import Optional

from psycopg.rows import dict_row

from diamm_indexer.helpers.db import postgres_pool


def get_latest_diamm_datetime() -> Optional[str]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute(
            """SELECT updated FROM diamm_data_source ORDER BY updated desc LIMIT 1;"""
        )
        res = curs.fetchone()

    updated_dt: Optional[datetime] = res.get("updated")
    if updated_dt:
        utc_tz = updated_dt.astimezone(datetime.UTC)
        return utc_tz.strftime("%Y-%m-%dT%H:%M:%SZ")

    return None
