import logging

import yaml
from psycopg_pool import ConnectionPool


log = logging.getLogger("muscat_indexer")
idx_config: dict = yaml.full_load(open('./index_config.yml', 'r'))

config: dict = {
    "user": idx_config['postgres']['username'],
    "password": idx_config['postgres']['password'],
    "db": idx_config['postgres']['database'],
    "host": idx_config['postgres']['server'],
}

server_connection: str
if idx_config['postgres']['server']:
    server_connection = f"hostaddr={config['host']}"
else:
    server_connection = ""


postgres_pool = ConnectionPool(
    f"{server_connection} dbname={config['db']} user={config['user']} password={config['password']}"
)
