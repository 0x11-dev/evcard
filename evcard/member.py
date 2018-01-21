# -*- coding: utf-8 -*-

import logging

import re
import arrow

from .datasource import get_impala_connection
from .cache import (get_org, load_month_order, get_order_summary_for_member, set_task_status, get_task_status)
from . import common as comm
from .config import get_config
from impala.error import HiveServer2Error

logger = logging.getLogger(__name__)

member_columns = {
    'auth_id': None, 'year': None, 'month': None, 'province': None, 'city':None, 'gender': 2,
    'birth_date': None, 'review_status': None, 'active_status': None, 'leave_status': None,
    'deposit': 0.0, 'cancel_order_count': 0, 'man_cancel_count': 0, 'sys_cancel_count': 0,
    'success_order_count': 0, 'cost_time': 0, 'mileage': 0, 'receivable_amount': 0.0,
    'real_amount': 0.0, 'pickup_num': 0, 'pickup_service_fee': 0.0, 'return_num': 0,
    'return_service_fee': 0.0, 'insurance_num': 0, 'insurance': 0.0, 'discount_num': 0,
    'discount': 0.0, 'exemption_num': 0, 'exemption_amount': 0.0, 'illegal_num': 0
}

id_card_pattern = r'^([1-9]\d{5}[12]\d{3}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])\d{3}[0-9xX])$'

_orders = dict()
_leave_threshold = get_config()['leave_threshold']

# _values = []
# for _col_name in member_columns:
#     _values.append('%({})s'.format(_col_name))

statement = 'UPSERT INTO kudu_db.ec_member_month({}) VALUES'.format(','.join(member_columns.keys()))


def process_membership(year, month):
    load_month_order(year, month)

    query = """
        select 
            auth_id,
            driver_code,
            org_id,
            gender,
            birth_date,
            review_status,
            review_time,
            reg_time,
            deposit
        from evcard_tmp.membership_info
        where reg_time<%(to_date)s and membership_type=0
    """

    date = arrow.Arrow(year, month, 1, tzinfo='local')

    set_task_status('member:solo', date.format('YYYYMM'), 'running')

    with get_impala_connection() as impala_conn:
        _gather_order(impala_conn, year, month)
        with impala_conn.cursor() as cursor:
            cursor.arraysize = comm.G_BATCH_SIZE

            params = {'to_date': date.shift(months=+1).format(comm.G_DATE_FORMATTER)}
            cursor.execute(query, params)
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

    set_task_status('member:solo', date.format('YYYYMM'), 'finished')

    return total_count


def _process_batch(conn, date, data):
    values = list()
    for row in data:
        if row.get('reg_time') is None or len(row.get('reg_time')) == 0:
            continue

        new_row = comm.init_row(member_columns)

        new_row['auth_id'] = row.get('auth_id')
        new_row['year'] = date.year
        new_row['month'] = date.month

        review_status = row.get('review_status')
        review_date = row.get('review_time')
        # if review_date:
        #     try:
        #         arrow.get(review_date, 'YYYYMMDDHHmmss')
        #     except arrow.parser.ParserError:
        #         print("error")
        if review_date and len(review_date) == 14:
            review_date = arrow.get(review_date, 'YYYYMMDD', tzinfo='local')
        else:
            review_date = arrow.get(row.get('reg_time'), 'YYYYMMDD', tzinfo='local')

        # -1:资料不全 0:待审核 1：审核通过 2: 审核不通过 3：用户无效 4:重新审核
        if review_status == 1:
            if review_date and review_date == date:
                new_row['review_status'] = 2    # 本月审核通过
            else:
                new_row['review_status'] = 3  # 审核通过
        elif review_status == 2:
            if review_date and review_date == date:
                new_row['review_status'] = 4    # 本月审核未通过
            else:
                new_row['review_status'] = 5  # 审核未通过

            new_row['active_status'] = 0    # 未通过审核
            new_row['leave_status'] = 0     # 未激活
        else:
            new_row['review_status'] = 1  # 注册
            new_row['active_status'] = 0    # 未通过审核
            new_row['leave_status'] = 0     # 未激活

        new_row['deposit'] = row.get('deposit')

        org = get_org(row.get('org_id'))
        if org:
            new_row['province'] = org.get('province_code')
            new_row['city'] = org.get('city_code')

        id_card = _parse_id_card(row.get('driver_code'))
        if row.get('birth_date'):
            new_row['birth_date'] = row.get('birth_date')
        elif id_card:
            new_row['birth_date'] = id_card.get('birth_date')

        gender = row.get('gender')
        if gender == 0 or gender == 1: # 男 or 女
            new_row['gender'] = gender
        else:
            new_row['gender'] = id_card.get('gender') if id_card else 2

        # df = get_order_for_member(date.year, date.month, new_row.get('auth_id'))
        # if not df.empty:
        _order = get_order_summary_for_member(date.year, date.month, new_row.get('auth_id'))
        if _order.get('cancel_order_count') is not None:
            # df['sky_hole'] = 'x'  # 防止sum时类型转换！
            # _order = df.sum(numeric_only=False)
            new_row['cancel_order_count'] = _order.get('cancel_order_count')
            new_row['man_cancel_count'] = _order.get('man_cancel_count')
            new_row['sys_cancel_count'] = _order.get('sys_cancel_count')
            new_row['success_order_count'] = _order.get('success_order_count')
            new_row['cost_time'] = _order.get('cost_time')
            new_row['mileage'] = _order.get('mileage')
            new_row['receivable_amount'] = _order.get('receivable_amount')
            new_row['real_amount'] = _order.get('real_amount')
            new_row['pickup_num'] = _order.get('pickup_num')
            new_row['pickup_service_fee'] = _order.get('pickup_service_fee')
            new_row['return_num'] = _order.get('return_num')
            new_row['return_service_fee'] = _order.get('return_service_fee')
            new_row['insurance_num'] = _order.get('insurance_num')
            new_row['insurance'] = _order.get('insurance')
            new_row['discount_num'] = _order.get('discount_num')
            new_row['discount'] = _order.get('discount')
            new_row['exemption_num'] = _order.get('exemption_num')
            new_row['exemption_amount'] = _order.get('exemption_amount')
            new_row['illegal_num'] = _order.get('illegal_num')

        if review_status == 1:  # 通过审核用户
            order_count = _orders.get(new_row.get('auth_id'))
            current_month_order_count = new_row.get('cancel_order_count') + new_row.get('success_order_count')

            if order_count and order_count[0] > 0:
                new_row['active_status'] = 3  # 本月之前有订单->已激活
                if order_count[1] > 0:
                    new_row['leave_status'] = 2  # N个月内有订单->留存
                else:
                    if current_month_order_count == 0:
                        new_row['leave_status'] = 1  # 连续N个月无订单->流失
            else:   # 本月之前无订单
                if current_month_order_count > 0:
                    new_row['active_status'] = 2 # 本月有订单->本月激活
                    new_row['leave_status'] = 2  # N个月内有订单->留存
                else:
                    new_row['active_status'] = 1 # 没有订单->休眠
                    new_row['leave_status'] = 0  # 未激活用户

        v = '({})'.format(','.join(map(comm.convert, new_row.values())))
        values.append(v)

    if values:
        insert_statement = '{}{}'.format(statement, ','.join(values))
        with conn.cursor() as cursor:
            try:
                cursor.execute(insert_statement)
            except HiveServer2Error as e:
                # print(insert_statement)
                # print(_order)
                # print(type(_order.get('cancel_order_count')))
                raise e


def _gather_order(conn, year, month):
    logger.debug("loading order summary of member... %s-%s", year, month)
    _orders.clear()

    query1 = """
      select auth_id,sum(cancel_order_count+success_order_count) as order_count
      from kudu_db.ec_member_month 
      where
        (active_status=2 or active_status=3) and 
        (year<%(to_year)s or (year=%(to_year)s and month<%(to_month)s))
      group by auth_id;
    """

    query2 = """
      select auth_id,sum(cancel_order_count+success_order_count) as order_count
      from kudu_db.ec_member_month 
      where
        (active_status=2 or active_status=3) and  
        (year>%(from_year)s or (year=%(from_year)s and month>=%(from_month)s)) and
        (year<%(to_year)s or (year=%(to_year)s and month<%(to_month)s))
      group by auth_id;
    """

    to_date = arrow.Arrow(year, month, 1)
    from_date = to_date.shift(months=-_leave_threshold)  # 最近6个月的订单数
    parameters = {'from_year': from_date.year, 'from_month': from_date.month, 'to_year': to_date.year, 'to_month': to_date.month}

    with conn.cursor() as cursor:
        cursor.execute(query1, parameters)

        _count = 0
        for row in cursor:
            _orders[row[0]] = [row[1], 0]
            _count += 1
            if _count % 10000 == 0:
                logger.debug('T:No.%s', _count)

        _count = 0
        cursor.execute(query2, parameters)
        for row in cursor:
            _orders.get(row[0])[1] = row[1]
            _count += 1
            if _count % 10000 == 0:
                logger.debug('6:No.%s', _count)


def _parse_id_card(id_card):
    if id_card and re.match(id_card_pattern, id_card):
        card_info = dict()
        card_info['birth_date'] = id_card[6:14]
        card_info['gender'] = 1 if int(id_card[16]) % 2 == 0 else 0
        return card_info

