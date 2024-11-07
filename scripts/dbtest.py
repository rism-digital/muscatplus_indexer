from typing import Generator

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

postgres_pool = ConnectionPool("dbname=diamm_data_server user=ahankins password=")


def _get_towns() -> Generator[dict, None, None]:
    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute("""SELECT id, code FROM Towns;""")

        while rows := curs.fetchmany(size=1000):
            yield rows


if __name__ == "__main__":
    table_q = """CREATE TABLE Towns (
        id SERIAL UNIQUE NOT NULL,
        code VARCHAR(16) NOT NULL, -- not unique
        article TEXT,
        name TEXT NOT NULL, -- not unique
        UNIQUE (code)
    );"""

    insert_q = """insert into towns (
        code, article, name
    )
    select
        left(md5(i::text), 16),
        md5(random()::text),
        md5(random()::text)
        from generate_series(1, 1000000) s(i);"""

    with postgres_pool.connection() as conn:
        curs = conn.cursor(row_factory=dict_row)
        curs.execute(table_q)
        curs.execute(insert_q)

    town_groups = _get_towns()
    for town_group in town_groups:
        print(len(town_group))
