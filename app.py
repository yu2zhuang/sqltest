import time
import logging
from functools import wraps
import argparse

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from orm_style import Base

from config import *
import orm_style
import sql_style

logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s - %(levelname)s: %(message)s")


def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        output, error = "NULL", "NULL"
        try:
            output = func(*args, **kwargs)
        except Exception as e:
            error = str(e)
        logging.info(
            "\n\t spends: {}s \n\t output: {} \n\t error: {}".format(
                round(time.time() - start, 4), output, error))

    return wrapper


@timer
def style_orm(src, dest):
    """测试 ORM包装SQL风格"""
    # 连接信息
    url_fmt = 'postgresql+psycopg2://{user}:{password}@{host}/{dbname}'
    url = url_fmt.format(**PG_CONN_KWARGS)
    engine = create_engine(url, echo=False)

    # 1. 初始化数据表
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    # 2. 数据入库
    orm_style.to_sql(src, session)
    # 3. 数据分析
    orm_style.update_table(session)
    # 4. 结果导出
    orm_style.to_csv(session, dest)
    return dest


@timer
def style_sql(src, dest):
    """python脚本控制 SQL 执行风格 """

    # 连接信息
    dsn_fmt = "host={host} user={user} password={password} dbname={dbname}"
    dsn = dsn_fmt.format(**PG_CONN_KWARGS)

    # 1. 初始化数据表
    sql_style.create_table(dsn)
    # 2. 数据入库
    sql_style.to_sql(src, dsn, TABLENAME, COLUMNS)
    # 3. 数据分析
    sql_style.update(dsn)
    # 4. 结果导出
    sql_style.to_csv(dsn, TABLENAME, dest)
    return dest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cell Traffic Stat')

    parser.add_argument('-t', dest='type', choices={'orm', 'sql'},
                        default='sql',
                        help='type to statistics')

    args = parser.parse_args()
    print('type to statistics: ', args.type)

    # src = "files/sample_10millions.zip"
    src = "files/sample_10thousands.zip"
    dest = "files/sample_output.csv"

    if args.type == "orm":
        style_orm(src, dest)
    else:
        style_sql(src, dest)

    # 终端执行 ：
    # 执行SQL风格代码：python app.py 等价于 python app.py -t sql
    # 执行ORM风格代码： python app.py -t orm
