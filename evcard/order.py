
# -*- coding: utf-8 -*-

import logging
import time
import arrow

from .datasource import (get_impala_connection, get_redis_connection)
from .cache import (get_area, get_order, get_org, clean_cache, set_task_status, get_task_status)
from . import common as comm
from .config import get_config
from .exceptions import EvcardException

logger = logging.getLogger(__name__)

# conn = connect(host='10.20.140.11', port=21050)

order_columns = [
    'order_seq', 'year', 'month', 'order_time', 'week_day', 'holiday', 'auth_id', 'org_id',
    'vin', 'vehicle_province', 'vehicle_city', 'payment_status', 'order_type',
    'activity_type', 'market_activity_type', 'pickup_store_seq',
    'pickup_province', 'pickup_city', 'pickup_district', 'return_store_seq',
    'return_province', 'return_city', 'return_district', 'star_level', 'cancel_flag',
    'org_free', 'cost_time', 'mileage', 'pay_way', 'receivable_amount', 'real_amount',
    'pickup_service_fee', 'return_service_fee', 'insurance', 'discount',
    'discount_rate', 'illegal_flag', 'exemption_amount'
]

# _values = list()
# for _col_name in order_columns:
#     _values.append('%({})s'.format(_col_name))

# _insert_statement = list(['UPSERT INTO kudu_db.ec_order_day('])
# _insert_statement.append('{})'.format(','.join(order_columns)))
# _insert_statement.append(' VALUES({})'.format(','.join(_values)))
# insert_statement = ''.join(_insert_statement)

redis_conn = get_redis_connection()

statement = 'UPSERT INTO kudu_db.ec_order_day({}) VALUES'.format(','.join(order_columns))

_stars = dict()


def _clean_cache(year, month, day):
    clean_cache('veh:*:{:0>4}{:0>2}{:0>2}'.format(year, month, day))
    clean_cache('shop:*:{:0>4}{:0>2}{:0>2}'.format(year, month, day))
    clean_cache('mem:*:{:0>4}{:0>2}:{:0>2}'.format(year, month, day))


def handle_order(year, month, day):
    logger.info("loading order data of %s-%s-%s ...", year, month, day)
    _clean_cache(year, month, day)

    query = '''
        select 
            o.order_seq,
            o.created_time,
            o.vin,
            v.org_id as vehicle_org_id,
            o.auth_id,
            o.org_id,
            o.pickup_store_seq,
            ps.area_code as pick_area_code,
            o.return_store_seq,
            rs.area_code as return_area_code,
            o.order_type,
            o.activity_type,
            o.market_activity_type,
            o.payment_status,
            o.cancel_flag,
            o.org_free,
            o.illegal_seq,
            o.cost_time,
            o.exemption_amount,
            op.insurance,
            o.unit_price,
            o.return_mileage,
            o.get_mileage,
            o.pay_way,
            o.memdiscount,
            o.pickveh_amount,
            o.returnveh_amount,
            o.discount,
            o.amount,
            o.real_amount,
            o.bill_time_precharge
        from evcard_tmp.order_info o
        left join evcard_tmp.vehicle_info v on v.vin = o.vin
        left join evcard_tmp.order_price_detail op on op.order_seq=o.order_seq 
        left join evcard_tmp.shop_info ps on ps.shop_seq=cast(o.pickup_store_seq as bigint)
        left join evcard_tmp.shop_info rs on rs.shop_seq=cast(o.return_store_seq as bigint)
        where substr(o.created_time,1,8)=%(order_date)s
        order by o.created_time
    '''

    date = arrow.Arrow(year, month, day, tzinfo='local')
    order_date = date.format('YYYYMMDD')
    # to_date = date.shift(days=+1).format(comm.G_DATE_FORMATTER)

    set_task_status('order', order_date, 'running')

    params = {'order_date': order_date}
    _load_star_level(params)

    with get_impala_connection() as impala_conn:
        with impala_conn.cursor() as cursor:
            cursor.arraysize = get_config()['batch_size']

            s_time = time.time()
            cursor.execute(query, params)
            e_time = time.time()
            logger.debug("execute finished, use time: %s sec", e_time-s_time)
            _columns = [metadata[0] for metadata in cursor.description]

            _page = 1
            total_count = 0
            grid = comm.as_grid(cursor, _columns)
            while len(grid) > 0:
                total_count += len(grid)
                _count = _process_batch(impala_conn, date, grid)
                logger.debug('process page %s(%s) %s...', _page, total_count, _count)
                grid = comm.as_grid(cursor, _columns)
                _page += 1

    set_task_status('order', order_date, 'finished')

    return total_count


def _process_batch(conn, date, data):
    _date = date.format('YYYYMMDD')
    values = list()
    for row in data:
        new_row = comm.create_null_row(order_columns)
        new_row['order_seq'] = row.get('order_seq')
        new_row['star_level'] = _stars.get(new_row['order_seq'])
        _status = row.get('payment_status')
        new_row['payment_status'] = _status

        new_row['order_time'] = row.get('created_time')
        new_row['year'] = date.year
        new_row['month'] = date.month
        new_row['week_day'] = date.isoweekday()
        if date.isoweekday() in [6, 7]:
            new_row['holiday'] = True
        else:
            if _date in comm.HOLIDAYS:
                new_row['holiday'] = True
            else:
                new_row['holiday'] = False

        org = get_org(row.get('vehicle_org_id'))
        new_row['vehicle_province'] = org.get('province_code')
        new_row['vehicle_city'] = org.get('city_code')

        new_row['cancel_flag'] = row.get('cancel_flag')
        new_row['order_type'] = row.get('order_type')
        new_row['activity_type'] = row.get('activity_type')
        new_row['market_activity_type'] = row.get('market_activity_type')

        if row.get('org_free'):
            new_row['org_free'] = True
        else:
            new_row['org_free'] = False

        if row.get('illegal_seq'):
            new_row['illegal_flag'] = True
        else:
            new_row['illegal_flag'] = False

        if row.get('cost_time') is not None:
            new_row['cost_time'] = int(row.get('cost_time'))
            if row.get('exemption_amount') is not None and row.get('unit_price') > 0:
                new_row['cost_time'] -= int(row.get('exemption_amount') / row.get('unit_price') / row.get('memdiscount'))

        if row.get('return_mileage') is not None:
            new_row['mileage'] = int(row.get('return_mileage'))
            if row.get('get_mileage') is not None:
                new_row['mileage'] -= int(row.get('get_mileage'))
                # TODO: if <0 的情况需要修正

        if row.get('pay_way') is not None:
            new_row['pay_way'] = int(row.get('pay_way'))

        new_row['discount_rate'] = row.get('memdiscount')
        new_row['discount'] = row.get('discount')
        new_row['pickup_service_fee'] = row.get('pickveh_amount')
        new_row['return_service_fee'] = row.get('returnveh_amount')
        new_row['exemption_amount'] = row.get('exemption_amount')
        new_row['insurance'] = row.get('insurance')

        # 应收金额
        if row.get('amount') is not None:
            new_row['receivable_amount'] = row.get('amount')
            if row.get('insurance') is not None:
                new_row['receivable_amount'] += row.get('insurance')
            if row.get('exemption_amount') is not None:
                new_row['receivable_amount'] -= row.get('exemption_amount')

        # 实收金额
        if row.get('real_amount') is not None:
            new_row['real_amount'] = row.get('real_amount')
            if row.get('bill_time_precharge') is not None:
                new_row['real_amount'] += row.get('bill_time_precharge')

        new_row['org_id'] = row.get('org_id')

        # 根据会员汇总
        _auth_id = row.get('auth_id')
        new_row['auth_id'] = _auth_id
        if _auth_id:
            _mem_key = 'mem:{}:{}:{}'.format(_auth_id, date.format('YYYYMM'), date.format('DD'))
            _order = get_order(_mem_key)
            if _status == 0:    # 取消次数
                _hincrby(_order, 'cancel_order_count')
                if row.get('cancel_flag') == 1:   # 手动取消
                    _hincrby(_order, 'man_cancel_count')
                elif row.get('cancel_flag') == 2:  # 系统取消
                    _hincrby(_order, 'sys_cancel_count')
            elif (_status == 5 or _status == 6) and (new_row['order_type'] == 0 or new_row['order_type'] == 2):  # 成功订单次数
                _hincrby(_order, 'success_order_count')
                if new_row['cost_time']:
                    _hincrby(_order, 'cost_time', new_row['cost_time'])
                if new_row['mileage']:
                    _hincrby(_order, 'mileage', new_row['mileage'])
                if new_row['receivable_amount']:
                    _hincrbyfloat(_order, 'receivable_amount', new_row['receivable_amount'])
                if new_row['real_amount']:
                    _hincrbyfloat(_order, 'real_amount', new_row['real_amount'])
                if new_row['pickup_service_fee']:   # 取车次数 & 服务费
                    _hincrby(_order, 'pickup_num')
                    _hincrbyfloat(_order, 'pickup_service_fee', new_row['pickup_service_fee'])
                if new_row['return_service_fee']: # 还车次数 & 服务费
                    _hincrby(_order, 'return_num')
                    _hincrbyfloat(_order, 'return_service_fee', new_row['return_service_fee'])
                if new_row['insurance']:    # 不计免赔次数 & 金额
                    _hincrby(_order, 'insurance_num')
                    _hincrbyfloat(_order, 'insurance', new_row['insurance'])
                if new_row['discount']: # 优惠次数 & 金额
                    _hincrby(_order, 'discount_num')
                    _hincrbyfloat(_order, 'discount', new_row['discount'])
                if new_row['exemption_amount']: # 减免次数 & 金额
                    _hincrby(_order, 'exemption_num')
                    _hincrbyfloat(_order, 'exemption_amount', new_row['exemption_amount'])
                if new_row['illegal_flag']:  #违章次数
                    _hincrby(_order, 'illegal_num')

            if len(_order) > 0:
                redis_conn.hmset(_mem_key, _order)

        # 根据车辆汇总
        new_row['vin'] = row.get('vin')
        if new_row['vin']:
            _veh_key = 'veh:{}:{}'.format(new_row['vin'], _date)
            _order = get_order(_veh_key)
            if new_row['illegal_flag']: # 违章次数
                _hincrby(_order, 'illegal_count')

            if new_row['order_type'] == 0 or new_row['order_type'] == 2:
                if _status == 0:
                    _hincrby(_order, 'cancel_order_count')
                elif _status == 5 or _status == 6:
                    _hincrby(_order, 'success_order_count')
                    if new_row['cost_time']:
                        _hincrby(_order, 'cost_time', new_row['cost_time'])
                    if new_row['mileage']:
                        _hincrby(_order, 'mileage', new_row['mileage'])
                    if new_row['receivable_amount']:
                        _hincrbyfloat(_order, 'receivable_amount', new_row['receivable_amount'])
                    if new_row['real_amount']:
                        _hincrbyfloat(_order, 'real_amount', new_row['real_amount'])

                    if new_row['market_activity_type'] != 1:    # 红包车
                        _hincrby(_order, 'red_bag_count')

                    if new_row['star_level'] == 0:
                        _hincrby(_order, 'star_zero')
                    elif new_row['star_level'] == 1:
                        _hincrby(_order, 'star_one')
                    elif new_row['star_level'] == 2:
                        _hincrby(_order, 'star_two')
                    elif new_row['star_level'] == 3:
                        _hincrby(_order, 'star_three')
                    elif new_row['star_level'] == 4:
                        _hincrby(_order, 'star_four')
                    elif new_row['star_level'] == 5:
                        _hincrby(_order, 'star_five')

            if len(_order) > 0:
                redis_conn.hmset(_veh_key, _order)

        # 根据取车网点汇总
        pickup_store = row.get('pickup_store_seq')
        if pickup_store is not None and str(pickup_store).isdigit():
            new_row['pickup_store_seq'] = int(pickup_store)
            if new_row['order_type'] == 0 or new_row['order_type'] == 2:
                _shop_key = 'shop:{}:{}'.format(new_row['pickup_store_seq'], _date)
                _order = get_order(_shop_key)
                _hincrby(_order, 'pickup_num')
                if _status == 0:
                    _hincrbyfloat(_order, 'cancel_order_count', 0.5)
                elif _status == 5 or _status == 6:
                    _hincrbyfloat(_order, 'success_order_count', 0.5)
                    if new_row['receivable_amount']:
                        _hincrbyfloat(_order, 'income', 0.5 * new_row['receivable_amount'])
                    if new_row['real_amount']:
                        _hincrbyfloat(_order, 'real_income', 0.5 * new_row['real_amount'])

                if len(_order) > 0:
                    redis_conn.hmset(_shop_key, _order)

            if row.get('pick_area_code') is not None:
                area = get_area(int(row.get('pick_area_code')))
                if area:
                    new_row['pickup_province'] = area.get('province_code')
                    new_row['pickup_city'] = area.get('city_code')
                    new_row['pickup_district'] = area.get('district_code')

        # 根据还车网点汇总
        return_store = row.get('return_store_seq')
        if return_store is not None and str(return_store).isdigit():
            new_row['return_store_seq'] = int(return_store)
            if new_row['order_type'] == 0 or new_row['order_type'] == 2:
                _shop_key = 'shop:{}:{}'.format(new_row['return_store_seq'], _date)
                _order = get_order(_shop_key)
                _hincrby(_order, 'return_num')
                if _status == 0:
                    _hincrbyfloat(_order, 'cancel_order_count', 0.5)
                elif _status == 5 or _status == 6:
                    _hincrbyfloat(_order, 'success_order_count', 0.5)
                    if new_row['receivable_amount']:
                        _hincrbyfloat(_order, 'income', 0.5 * new_row['receivable_amount'])
                    if new_row['real_amount']:
                        _hincrbyfloat(_order, 'real_income', 0.5 * new_row['real_amount'])

                if len(_order) > 0:
                    redis_conn.hmset(_shop_key, _order)

            if row.get('return_area_code') is not None:
                area = get_area(int(row.get('return_area_code')))
                if area:
                    new_row['return_province'] = area.get('province_code')
                    new_row['return_city'] = area.get('city_code')
                    new_row['return_district'] = area.get('district_code')

        v = '({})'.format(','.join(map(comm.convert, new_row.values())))
        values.append(v)

    if values:
        insert_statement = '{}{}'.format(statement, ','.join(values))
        with conn.cursor() as cursor:
            cursor.execute(insert_statement)
        return len(values)
    else:
        return 0


def _load_star_level(order_date):
    logger.info("loading assess of order...")

    query = """
        select 
          order_seq as seq,
          star_level as level
        from evcard_tmp.assess_info
        where substr(created_time,1,8)=%(order_date)s
        order by created_time        
    """
    _stars.clear()

    with get_impala_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, order_date)
            for row in cur:
                _stars[row[0]] = row[1]

    logger.info("loaded assess(%s).", len(_stars))


def _hincrby(order, field, value=1):
    if order.get(field) is None:
        order[field] = value
    else:
        order[field] = int(order.get(field)) + value


def _hincrbyfloat(order, field, value):
    if order.get(field) is None:
        order[field] = value
    else:
        order[field] = float(order.get(field)) + value

