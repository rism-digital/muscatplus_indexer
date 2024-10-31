import datetime
from typing import Optional

from psycopg.rows import dict_row

from cantus_indexer.helpers.db import postgres_pool


def get_latest_cantus_datetime() -> Optional[str]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute(
            """SELECT date_updated FROM main_app_source ORDER BY date_updated desc LIMIT 1;"""
        )
        res = curs.fetchone()

    updated_dt: Optional[datetime] = res.get("date_updated")
    if updated_dt:
        utc_tz = updated_dt.astimezone(datetime.UTC)
        return utc_tz.strftime("%Y-%m-%dT%H:%M:%SZ")

    return None
