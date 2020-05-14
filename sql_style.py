from io import StringIO
import traceback
import logging

import psycopg2
import pandas as pd


def execute(dsn, sql):
    """ 一次性执行SQL语句 """
    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cursor:
            # 无需commit，with语句执行完成后自动commit
            # commit() method is automatically called if no exception is raised in the with block.
            # rollback() method is automatically called if an exception is raised in the with block.
            cursor.execute(sql)


def create_table(dsn):
    """ 执行建表SQL """
    sql = """ 
    drop table if exists cell_traffic_stat;
    create table cell_traffic_stat(
        id serial,
        cell_id varchar(24),
        date timestamp without time zone,
        tel_traffic double precision,
        traffic double precision,
        user_num integer,
        CONSTRAINT cell_traffic_stat_pkey PRIMARY KEY (id)
        ) ;
        """
    execute(dsn, sql)
    logging.info(sql)


def chunks(filepath, columns, chunksize=500000):
    """
    将大文件分片，读取到StringIO中的生成器函数
    :param filepath: 文件路径
    :param columns: 处理的列
    :param chunksize: 一次性处理行数
    :return: StringIO对象
    """
    reader = pd.read_csv(filepath, header=0, chunksize=chunksize, dtype='object')
    for df in reader:
        sio = StringIO()
        df = df[columns]
        df.to_csv(sio, index=False, header=False,sep=',')
        sio.seek(0)
        yield sio


def copy2db(sio, dsn, tablename, columns):
    """
    将StringIO对象数据写入数据表
    :param sio: StringIO对象
    :param dsn: 数据库链接信息dsn
    :param tablename: 表名
    :param columns: 选取的列
    :return: None
    """
    copy_sql = "copy {} ({}) from STDIN WITH DELIMITER AS ',' CSV".format(tablename, ','.join(columns))
    conn = psycopg2.connect(dsn)
    with conn:
        cur = conn.cursor()
        try:
            cur.copy_expert(copy_sql, sio)
            logging.info("{} {} lines".format(copy_sql, cur.rowcount))
        except:
            print(str(traceback.format_exc()).replace('\'', '\'\''))
        finally:
            sio.close()


def to_sql(filepath, dsn, tablename, columns):
    """
    控制入库功能
    """
    for batch in chunks(filepath, columns):
        copy2db(batch, dsn, tablename, columns)


def update(dsn):
    """ 执行更新操作的SQL """
    sql = """
    alter table cell_traffic_stat add column "sum_traffic" double precision;
    alter table cell_traffic_stat add column "avg_traffic" double precision;
    update cell_traffic_stat set sum_traffic = traffic + tel_traffic;
    update cell_traffic_stat set avg_traffic = (case when user_num = 0 then 0 else sum_traffic/user_num end);
    update cell_traffic_stat set "date"=("date" - interval '3 hour');
    """
    execute(dsn, sql)
    logging.info(sql)


def to_csv(dsn, tablename, output):
    """
    没有使用pandas的read_sql功能，而是使用普通的fetchall方法，
    自己合成dataframe对象，生成结果文件。
    :param dsn: 数据库连接信息
    :param tablename: 需要导出数据的数据表
    :param output: 输出文件路径
    :return: None
    """
    sql = """select * from {}""".format(tablename)
    with psycopg2.connect(dsn) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows)
        df.columns = cols
    df.to_csv(output, header=True, index=False)

