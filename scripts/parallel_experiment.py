from indexer.helpers.db import mysql_pool
import concurrent.futures
from typing import List, Generator


def _do_query() -> Generator:
    conn = mysql_pool.connection()
    curs = conn.cursor()
    res: List[List] = []

    curs.execute(
        """SELECT id, marc_source FROM muscat_development.sources WHERE source_id IS NULL;"""
    )

    while rows := curs._cursor.fetchmany(1000):
        yield rows


def process_results(group: List, gnum: int) -> str:
    print(f"Processing group # {gnum}")

    return f"Group {gnum}"


def main() -> bool:
    results = _do_query()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures_list = [
            executor.submit(process_results, record, gp)
            for gp, record in enumerate(results)
        ]

        for f in concurrent.futures.as_completed(futures_list):
            f.result()

    return True


if __name__ == "__main__":
    main()
