# -*- coding: utf-8 -*-

import os
import logging
import sqlite3
import pandas as pd

from .datasource import (get_redis_connection, get_impala_connection)
from . import common as comm

logger = logging.getLogger(__name__)

query_area = """
    select 
      a.areaid,a.area,c.cityid,c.city,p.provinceid,p.province
    from evcard_tmp.area a
    left join evcard_tmp.city c on a.fatherid=c.cityid
    left join evcard_tmp.province p on c.fatherid=p.provinceid
"""

query_org = """
    select
        org_id,province_code,city_code
    from kudu_db.ec_org_info
"""

redis_conn = get_redis_connection()


def load_area_data():
    logger.info('loading area data...')
    for key in redis_conn.keys('area:*'):
        redis_conn.delete(key)

    with get_impala_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query_area)
            for row in cursor:
                key = 'area:' + str(row[0])
                value = dict()
                value['district_code'] = row[0]
                value['district_name'] = row[1]
                value['city_code'] = row[2]
                value['city_name'] = row[3]
                value['province_code'] = row[4]
                value['province_name'] = row[5]
                redis_conn.hmset(key, value)


def load_org_data():
    logger.info('loading org data...')
    for key in redis_conn.keys('org:*'):
        redis_conn.delete(key)

    with get_impala_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query_org)
            for row in cursor:
                key = 'org:' + str(row[0])
                value = dict()
                if row[1] is not None:
                    value['province_code'] = row[1]
                if row[2] is not None:
                    value['city_code'] = row[2]

                if len(value) > 0:
                    redis_conn.hmset(key, value)


def get_area(area_id):
    return redis_conn.hgetall('area:' + str(area_id))


def get_org(org_id):
    return redis_conn.hgetall('org:' + str(org_id))


def get_shop_order(date, shop_id):
    return redis_conn.hgetall('shop:{}:{}'.format(shop_id, date))


def get_vehicle_order(date, vehicle_id):
    return redis_conn.hgetall('veh:{}:{}'.format(vehicle_id, date))


def get_order(key):
    return redis_conn.hgetall(key)


def set_task_status(key, day, status):
    redis_conn.set('status:{}:{}'.format(key, day), status)


def get_task_status(key, day):
    return redis_conn.get('status:{}:{}'.format(key, day))


def clean_cache(pattern):
    for key in redis_conn.keys(pattern):
        redis_conn.delete(key)


_order_columns = {
    'auth_id': None, 'day': None,
    'cancel_order_count': 0, 'man_cancel_count': 0, 'sys_cancel_count': 0,
    'success_order_count': 0, 'cost_time': 0, 'mileage': 0, 'receivable_amount': 0.0,
    'real_amount': 0.0, 'pickup_num': 0, 'pickup_service_fee': 0.0, 'return_num': 0,
    'return_service_fee': 0.0, 'insurance_num': 0, 'insurance': 0.0, 'discount_num': 0,
    'discount': 0.0, 'exemption_num': 0, 'exemption_amount': 0.0, 'illegal_num': 0
}


def _get_connect(year, month):
    db = '~/.evcard/cache/orders_{}_{}.db'.format(year, month)
    db_path = os.path.expanduser(db)
    return sqlite3.connect(db_path)


def load_month_order(year, month):
    logger.info('loading month order data...')

    sqlite_conn = _get_connect(year, month)

    create_table_statement = '''
        create table order_ (
            auth_id text not null,
            day int not null,
            cancel_order_count int,
            man_cancel_count int,
            sys_cancel_count int,
            success_order_count int,
            cost_time int,
            mileage int,
            receivable_amount real,
            real_amount real,
            pickup_num int,
            pickup_service_fee real,
            return_num int,
            return_service_fee real,
            insurance_num int,
            insurance real,
            discount_num int,
            discount real,
            exemption_num int,
            exemption_amount real,
            illegal_num int,
            primary key(auth_id, day)
        );
    '''

    insert_statement = '''
        insert into order_(
          auth_id,day,cancel_order_count,man_cancel_count,sys_cancel_count,
          success_order_count,cost_time,mileage,receivable_amount,real_amount,
          pickup_num,pickup_service_fee,return_num,return_service_fee,insurance_num,
          insurance,discount_num,discount,exemption_num,exemption_amount,illegal_num
        ) values(
          :auth_id,:day,:cancel_order_count,:man_cancel_count,:sys_cancel_count,
          :success_order_count,:cost_time,:mileage,:receivable_amount,:real_amount,
          :pickup_num,:pickup_service_fee,:return_num,:return_service_fee,:insurance_num,
          :insurance,:discount_num,:discount,:exemption_num,:exemption_amount,:illegal_num        
        )
    '''

    cur = sqlite_conn.cursor()
    cur.execute('drop table if exists order_'.format(year, month))
    sqlite_conn.commit()
    cur.execute(create_table_statement)
    sqlite_conn.commit()

    count = 0
    values = list()
    key_pattern = 'mem:*:{:0>4}{:0>2}:*'.format(year, month)
    logger.debug("key like %s", key_pattern)
    for key in redis_conn.keys(key_pattern):
        row = comm.init_row(_order_columns)

        key_column = key.split(':')
        row['auth_id'] = key_column[1]
        row['day'] = int(key_column[3])

        _order = redis_conn.hgetall(key)

        row['cancel_order_count'] = int(_order.get('cancel_order_count') or 0)
        row['man_cancel_count'] = int(_order.get('man_cancel_count') or 0)
        row['sys_cancel_count'] = int(_order.get('sys_cancel_count') or 0)
        row['success_order_count'] = int(_order.get('success_order_count') or 0)
        row['cost_time'] = int(_order.get('cost_time') or 0)
        row['mileage'] = int(_order.get('mileage') or 0)
        row['receivable_amount'] = float(_order.get('receivable_amount') or 0)
        row['real_amount'] = float(_order.get('real_amount') or 0)
        row['pickup_num'] = int(_order.get('pickup_num') or 0)
        row['pickup_service_fee'] = float(_order.get('pickup_service_fee') or 0)
        row['return_num'] = int(_order.get('return_num') or 0)
        row['return_service_fee'] = float(_order.get('return_service_fee') or 0)
        row['insurance_num'] = int(_order.get('insurance_num') or 0)
        row['insurance'] = float(_order.get('insurance') or 0)
        row['discount_num'] = int(_order.get('discount_num') or 0)
        row['discount'] = float(_order.get('discount') or 0)
        row['exemption_num'] = int(_order.get('exemption_num') or 0)
        row['exemption_amount'] = float(_order.get('exemption_amount') or 0)
        row['illegal_num'] = int(_order.get('illegal_num') or 0)

        count += 1
        if count % 10000 == 0:
            logger.debug('loaded #%s...', count)

        values.append(row)
        if len(values) == 100:
            cur.executemany(insert_statement, values)
            sqlite_conn.commit()
            values.clear()

    if len(values) > 0:
        cur.executemany(insert_statement, values)
        sqlite_conn.commit()

    cur.close()
    sqlite_conn.close()
    logger.info('loaded month order data %s', count)


def get_order_for_member(year, month, auth_id):

    sqlite_conn = _get_connect(year, month)

    query_statement = '''
        select *
        from order_ 
        where auth_id=:auth_id
    '''

    df = pd.read_sql_query(query_statement, sqlite_conn, params={'auth_id': auth_id})
    sqlite_conn.close()

    return df


def get_order_summary_for_member(year, month, auth_id):
    sqlite_conn = _get_connect(year, month)
    cur = sqlite_conn.cursor()
    query_statement = '''
        select
            sum(cancel_order_count) as cancel_order_count,
            sum(man_cancel_count) as man_cancel_count,
            sum(sys_cancel_count) as sys_cancel_count,
            sum(success_order_count) as success_order_count,
            sum(cost_time) as cost_time,
            sum(mileage) as mileage,
            sum(receivable_amount) as receivable_amount,
            sum(real_amount) as real_amount,
            sum(pickup_num) as pickup_num,
            sum(pickup_service_fee) as pickup_service_fee,
            sum(return_num) as return_num,
            sum(return_service_fee) as return_service_fee,
            sum(insurance_num) as insurance_num,
            sum(insurance) as insurance,
            sum(discount_num) as discount_num,
            sum(discount) as discount,
            sum(exemption_num) as exemption_num,
            sum(exemption_amount) as exemption_amount,
            sum(illegal_num) as illegal_num   
        from order_ 
        where auth_id=:auth_id
    '''

    cur.execute(query_statement, {'auth_id': auth_id})
    _columns = [metadata[0] for metadata in cur.description]

    rs = comm.as_grid(cur, _columns, 1)

    cur.close()
    sqlite_conn.close()

    return rs
