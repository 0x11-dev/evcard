# -*- coding: utf-8 -*-

import logging

import arrow
from .datasource import get_impala_connection
from .cache import (get_area, get_shop_order, get_task_status, set_task_status)
from . import common as comm
from .config import get_config

logger = logging.getLogger(__name__)

shop_columns = [
    'shop_seq', 'year', 'month', 'day', 'province', 'city', 'district',
    'shop_type', 'shop_status', 'park_num', 'stack_num',
    'cancel_order_count', 'success_order_count', 'pickup_num',
    'return_num', 'income', 'real_income', 'turnover', 'park_income', 'park_real_income'
]


# _values = []
# for _col_name in shop_columns:
#     _values.append('%({})s'.format(_col_name))

# _insert_statement = list(['UPSERT INTO kudu_db.ec_shop_day('])
# _insert_statement.append('{})'.format(','.join(shop_columns)))
# _insert_statement.append(' VALUES({})'.format(','.join(_values)))
# insert_statement = ''.join(_insert_statement)
statement = 'UPSERT INTO kudu_db.ec_shop_day({}) VALUES'.format(','.join(shop_columns))


def process_shop(year, month, day):
    logger.info("processing shop data of %s-%s-%s ...", year, month, day)
    query = """
        select 
            shop_seq,
            area_code,
            shop_type,
            shop_status,
            park_num,
            stack_num
        from evcard_tmp.shop_info
        where first_online_time<%(to_date)s
    """

    date = arrow.Arrow(year, month, day, tzinfo='local')
    to_date = date.shift(days=+1).format('YYYY-MM-DD HH:mm:ss')

    set_task_status('shop', date.format('YYYYMMDD'), 'running')

    with get_impala_connection() as impala_conn:
        with impala_conn.cursor() as cursor:
            cursor.arraysize = get_config()['batch_size']

            cursor.execute(query, {'to_date': to_date})
            _columns = [metadata[0] for metadata in cursor.description]

            _page = 1
            total_count = 0
            grid = comm.as_grid(cursor, _columns)
            while len(grid) > 0:
                total_count += len(grid)
                logger.debug('process page %s(%s)...', _page, total_count)
                _process_batch(impala_conn, date, grid)
                grid = comm.as_grid(cursor, _columns)
                _page += 1

    set_task_status('shop', date.format('YYYYMMDD'), 'finished')

    return total_count


def _process_batch(conn, date, data):
    _date = date.format('YYYYMMDD')
    values = list()
    for row in data:
        new_row = comm.create_null_row(shop_columns)
        new_row['shop_seq'] = row.get('shop_seq')
        new_row['year'] = date.year
        new_row['month'] = date.month
        new_row['day'] = date.day

        area = get_area(row['area_code'])
        if area:
            new_row['province'] = area.get('province_code')
            new_row['city'] = area.get('city_code')
            new_row['district'] = area.get('district_code')

        new_row['shop_type'] = row.get('shop_type')
        new_row['shop_status'] = row.get('shop_status')
        new_row['park_num'] = row.get('park_num')
        new_row['stack_num'] = row.get('stack_num')

        order = get_shop_order(_date, row.get('shop_seq'))
        new_row['cancel_order_count'] = float(order.get('cancel_order_count') or 0.0)
        new_row['success_order_count'] = float(order.get('success_order_count') or 0.0)
        new_row['pickup_num'] = int(order.get('pickup_num') or 0)
        new_row['return_num'] = int(order.get('return_num') or 0)
        new_row['income'] = float(order.get('income') or 0.0)
        new_row['real_income'] = float(order.get('real_income') or 0.0)

        if new_row['stack_num']:
            new_row['turnover'] = new_row['success_order_count'] / new_row['stack_num']
            new_row['park_income'] = new_row['income'] / new_row['stack_num']
            new_row['park_real_income'] = new_row['real_income'] / new_row['stack_num']

        v = '({})'.format(','.join(
            map(lambda x: 'null' if x is None else "'{}'".format(x) if isinstance(x, str) else str(x), new_row.values())))

        values.append(v)

    if len(values):
        insert_statement = '{}{}'.format(statement, ','.join(values))
        with conn.cursor() as cursor:
            cursor.execute(insert_statement)

