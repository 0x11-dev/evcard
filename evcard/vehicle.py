# -*- coding: utf-8 -*-

import logging

import arrow
from impala.error import HiveServer2Error
from .datasource import get_impala_connection
from .cache import (get_vehicle_order, get_org, set_task_status, get_task_status)
from . import common as comm
from .config import get_config

logger = logging.getLogger(__name__)

vehicle_columns = [
    'vin', 'year', 'month', 'day', 'vehicle_model_seq', 'vehicle_model_name',
    'org_id', 'province', 'city', 'rent_type', 'operate_status',
    'illegal_count', 'fault_time', 'offline_time', 'online_rate',
    'cancel_order_count', 'success_order_count', 'cost_time', 'mileage',
    'receivable_amount', 'real_amount', 'red_bag_count',
    'star_one', 'star_two', 'star_three', 'star_four', 'star_five', 'star_zero'
]

# _values = []
# for _col_name in vehicle_columns:
#     _values.append('%({})s'.format(_col_name))

statement = 'UPSERT INTO kudu_db.ec_vehicle_day({}) VALUES'.format(','.join(vehicle_columns))

_fault = dict()


def process_vehicle(year, month, day):
    logger.info("processing vehicle data of %s-%s-%s ...", year, month, day)
    date = arrow.Arrow(year, month, day, tzinfo='local')

    set_task_status('vehicle', date.format('YYYYMMDD'), 'running')

    query = """
        select 
            v.vin,
            v.vehicle_model_seq,
            vm.vehicle_model_info,
            v.org_id,
            v.renttype,
            v.vehicleno_type,
            v.register_date,
            v.created_time
        from evcard_tmp.vehicle_info v
        left join evcard_tmp.vehicle_model vm on vm.vehicle_model_seq = v.vehicle_model_seq
    """

    to_date = date.shift(days=+1).format('YYYYMMDDHHmmssSSS')

    with get_impala_connection() as impala_conn:
        _load_offline_data(impala_conn, date)

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

    set_task_status('vehicle', date.format('YYYYMMDD'), 'finished')

    return total_count


def _process_batch(conn, date, data):
    _date = date.format('YYYYMMDD')
    values = list()
    for row in data:
        new_row = comm.create_null_row(vehicle_columns)
        new_row['vin'] = row.get('vin')

        # 计算运营状态
        if row.get('register_date') is None:
            _register_date = arrow.get(row.get('created_time')[0:8], 'YYYYMMDD', tzinfo='local')
        else:
            _date_len = len(row.get('register_date'))
            if _date_len == 7:
                _register_date = arrow.get(row.get('register_date') + '-01', 'YYYY-MM-DD', tzinfo='local')
            elif _date_len == 10:
                _register_date = arrow.get(row.get('register_date'), 'YYYY-MM-DD', tzinfo='local')
            else:
                _register_date = arrow.get(row.get('created_time')[0:8], 'YYYYMMDD', tzinfo='local')

        if _register_date > date:  # 未运营
            continue

        if row.get('vehicleno_type') is None or row.get('vehicleno_type') == 1:
            new_row['operate_status'] = 1   # 非租赁
        else: # vehicleno_type == 0
            if _register_date < date:
                new_row['operate_status'] = 3   # 已运营
            else: # 注册日期等于当前日期
                new_row['operate_status'] = 2   # 新增运营

        _fault_vin = _fault.get(new_row.get('vin'))
        if _fault_vin:
            new_row['offline_time'] = _fault_vin[0]
            new_row['fault_time'] = _fault_vin[1]
            new_row['online_rate'] = (1440 - new_row.get('offline_time') - new_row.get('fault_time')) / 14.4
        else:
            new_row['offline_time'] = 0
            new_row['fault_time'] = 0
            new_row['online_rate'] = 100

        new_row['year'] = date.year
        new_row['month'] = date.month
        new_row['day'] = date.day

        new_row['org_id'] = row.get('org_id')
        new_row['vehicle_model_seq'] = row.get('vehicle_model_seq')
        new_row['vehicle_model_name'] = row.get('vehicle_model_info')

        org = get_org(new_row['org_id'])
        if org:
            new_row['province'] = org.get('province_code')
            new_row['city'] = org.get('city_code')
        else:
            new_row['province'] = 'missing'
            new_row['city'] = 'missing'

        new_row['rent_type'] = row.get('renttype')

        order = get_vehicle_order(_date, new_row['vin'])
        new_row['cancel_order_count'] = int(order.get('cancel_order_count') or 0)
        new_row['success_order_count'] = int(order.get('success_order_count') or 0)
        new_row['cost_time'] = int(order.get('cost_time') or 0)
        new_row['mileage'] = int(order.get('mileage') or 0)
        new_row['receivable_amount'] = float(order.get('receivable_amount') or 0.0)
        new_row['real_amount'] = float(order.get('real_amount') or 0.0)
        new_row['illegal_count'] = int(order.get('illegal_count') or 0)
        new_row['red_bag_count'] = int(order.get('red_bag_count') or 0)
        new_row['star_one'] = int(order.get('star_one') or 0)
        new_row['star_two'] = int(order.get('star_two') or 0)
        new_row['star_three'] = int(order.get('star_three') or 0)
        new_row['star_four'] = int(order.get('star_four') or 0)
        new_row['star_five'] = int(order.get('star_five') or 0)
        new_row['star_zero'] = int(order.get('star_zero') or 0)

        v = '({})'.format(','.join(map(comm.convert, new_row.values())))
        values.append(v)

    if values:
        insert_statement = '{}{}'.format(statement, ','.join(values))
        with conn.cursor() as cursor:
            try:
                cursor.execute(insert_statement)
            except HiveServer2Error as e:
                op = cursor._last_operation
                logger.error("Error to execute statement %s", op.get_log(), exc_info=True)


def _load_offline_data(conn, today):
    logger.info("loading offline vehicle %s", today)

    query = """
        select off_time,recover_time,off_type,vin
        from evcard_tmp.vehicle_offline_info
        where off_time<%(end_time)s and (recover_time>=%(begin_time)s or recover_time is null)
    """

    _fault.clear()

    _time_formatter = 'YYYY-MM-DD HH:mm:ss'
    begin_time = today.format(_time_formatter)
    next_day = today.shift(days=+1)
    end_time = next_day.format(_time_formatter)
    parameters = {'begin_time': begin_time, 'end_time': end_time}

    with conn.cursor() as cursor:
        cursor.execute(query, parameters)
        for row in cursor:
            _start = arrow.get(row[0], _time_formatter)
            _start = today if _start < today else _start

            if row[1]:
                _end = arrow.get(row[1], _time_formatter)
                _end = next_day if _end >= next_day else _end
            else:
                _end = next_day

            delta = _end - _start
            _delta = delta.days * 1440 + delta.seconds // 60

            _vin = _fault.get(row[3])
            if _vin is None:
                _vin = [0, 0]
                _fault[row[3]] = _vin

            if row[2] == 1:  # 下线时间
                _vin[0] += _delta
            else:   # 2：故障时间
                _vin[1] += _delta
