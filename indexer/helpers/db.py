from typing import Dict

import MySQLdb
import yaml
from MySQLdb.cursors import SSDictCursor, SSCursor
from DBUtils.PooledDB import PooledDB

idx_config: Dict = yaml.full_load(open('index_config.yml', 'r'))

config: Dict = {
    "user": idx_config['mysql']['username'],
    "password": idx_config['mysql']['password'],
    "db": idx_config['mysql']['database']
}


mysql_connection = MySQLdb.connect(
    **config,
    host=idx_config['mysql']['server'],
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
