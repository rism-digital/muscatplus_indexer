import MySQLdb
import yaml
from MySQLdb.cursors import SSDictCursor, SSCursor
from dbutils.pooled_db import PooledDB

idx_config: dict = yaml.full_load(open('index_config.yml', 'r'))

config: dict = {
    "user": idx_config['mysql']['username'],
    "password": idx_config['mysql']['password'],
    "db": idx_config['mysql']['database'],
    "host": idx_config['mysql']['server'],
}


mysql_connection = MySQLdb.connect(
    **config,
    cursorclass=SSDictCursor
)

mysql_pool = PooledDB(
    **config,
    creator=MySQLdb,
    cursorclass=SSDictCursor,
    maxconnections=6,
    charset="utf8mb4",
    use_unicode=True
)
