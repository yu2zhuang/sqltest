# orm / sqlalchemy sql expression style
# 本人使用ORM较少，此示例仅做尝试
# ORM风格需要提前定义好模板，不适用于太灵活的场景

import pandas as pd
from sqlalchemy import Column, Sequence
from sqlalchemy import Integer, String, Float, DateTime, MetaData, Table
from sqlalchemy import case
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class CellTrafficStat(Base):
    __tablename__ = "cell_traffic_stat"
    id = Column(Integer, Sequence('cell_traffic_stat_id_seq'), primary_key=True)
    cell_id = Column(String, comment="小区名")
    date = Column(DateTime, comment="统计时间（小时级别）")
    tel_traffic = Column(Float, comment="话务流量")
    traffic = Column(Float, comment="网络流量")
    user_num = Column(Integer, comment="用户数")
    sum_traffic = Column(Float, comment="总业务量")
    avg_traffic = Column(Float, comment="平局业务量")

    def __repr__(self):
        return "<CellTraffic(date={}, cell_id={}, tel_traffic={}, traffic={}, user_num={})>".format(
            self.date,
            self.cell_id,
            self.tel_traffic,
            self.traffic,
            self.user_num)


metadata = MetaData()
tab = Table('cell_traffic_stat', metadata,
            Column('id', Integer, Sequence('cell_traffic_stat_id_seq'), primary_key=True),
            Column('cell_id', String),
            Column('date', DateTime),
            Column('tel_traffic', Float),
            Column('traffic', Float),
            Column('user_num', Integer),
            Column('sum_traffic', Float),
            Column('avg_traffic', Float), )


def _to_model(row, session):
    obj = CellTrafficStat(
        cell_id=row.cell_id,
        date=row.date,
        tel_traffic=row.tel_traffic,
        traffic=row.traffic,
        user_num=row.user_num)
    session.add(obj)


def to_sql(csv_file, session, chunksize=500000):
    reader = pd.read_csv(csv_file, compression="zip", header=0, chunksize=chunksize)
    for df in reader:
        df.apply(axis=1, func=lambda row: _to_model(row, session))
        session.commit()


def update_table(session):
    sum_traffic_exp = (tab.c.tel_traffic + tab.c.traffic).label("sum_traffic")
    avg_traffic_exp = (case([(tab.c.user_num == 0, 0), ],
                            else_=(tab.c.tel_traffic + tab.c.traffic) / tab.c.user_num)).label("avg_traffic")
    sql = tab.update().values(sum_traffic=sum_traffic_exp, avg_traffic=avg_traffic_exp)
    session.execute(sql)
    session.commit()


def to_csv(session, output):
    sql = tab.select()
    rset = session.execute(sql)
    res = [dict(row.items()) for row in rset]
    df = pd.DataFrame(res)
    df.to_csv(output, header=True, index=False)
