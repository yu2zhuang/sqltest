# postgres节点
PG_CONN_KWARGS = {
    "host": "localhost",
    "user": "postgres",
    "port": 5432,
    "password": "postgres",
    "dbname": "sqltest"
}

# 数据表表名
TABLENAME = "cell_traffic_stat"

# 对csv文件进行列过滤
COLUMNS = [
    "cell_id",
    "date",
    "tel_traffic",
    "traffic",
    "user_num"
]

