import MySQLdb
from MySQLdb.cursors import SSDictCursor, SSCursor
from DBUtils.PooledDB import PooledDB

mysql_connection = MySQLdb.connect(
    user="muscat",
    password="muscat",
    host="localhost",
    db="muscat_development",
    cursorclass=SSDictCursor
)

mysql_pool = PooledDB(
    creator=MySQLdb,
    user="muscat",
    password="muscat",
    db="muscat_development",
    cursorclass=SSDictCursor,
    maxconnections=6,
    charset="utf8mb4",
    use_unicode=True
)
