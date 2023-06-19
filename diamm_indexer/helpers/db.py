from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

postgres_pool = ConnectionPool(
    "dbname=diamm_data_server user=ahankins"
)
