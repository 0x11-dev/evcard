
# -*- coding: utf-8 -*-

import logging
import arrow

from .datasource import get_impala_connection
from . import common as comm

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

index_names = {
    1001: '取消订单数', 1002: '成功订单数', 1003: '企业订单数', 1004: '个人订单数', 1005: '夜租订单数',
    1006: '日租订单数', 1007: '违章次数', 1008: '分时订单数', 1009: '工作日订单数', 1010: '节假日订单数',
    1011: '时长', 1012: '距离', 1013: '应收金额', 1014: '实收金额', 1015: '取车费用', 1016: '还车费用',
    1017: '不计免赔次数', 1018: '不计免赔金额', 1019: '优惠次数', 1020: '优惠金额', 1021: '减免次数',
    1022: '减免金额', 1023: '1星评价数', 1024: '2星评价数', 1025: '3星评价数', 1026: '4星评价数',
    1027: '5星评价数', 1028: '0星评价数',
    2001: '注册会员数', 2002: '男会员', 2003: '女会员', 2004: '20+岁', 2005: '30+岁', 2006: '40+岁',
    2007: '50+岁', 2008: '其他年龄', 2009: '审核通过', 2010: '审核未通过', 2011: '押金会员数',
    2012: '激活数', 2013: '休眠数', 2014: '流失数', 2015: '留存数', 2016: '当月活跃数', 2017: '当月高频数',
    2018: '押金用户占比', 2019: '用户车辆比', 2020: '用户网点比', 2021: '用户车位比',
    3001: '网点总数', 3002: '总泊位数', 3003: '日均网点订单数', 3004: '日均网点收入', 3005: '日均网点实收',
    3006: '日均泊位周转率', 3007: '日均泊位收入', 3008: '车辆网点比', 3009: '网点车位比', 3010: '车辆车位比',
    4001: '车辆总数', 4002: '平均上线率', 4003: '平均服务天数', 4004: '红包车次数', 4005: '单车单日订单',
    4006: '单车单日时长', 4007: '单车单日收入', 4008: '单车单日实收', 4009: '单车单日距离', 4010: '故障次数',
    4011: '违章次数'
}

index_columns = [
    'index_id', 'year', 'month', 'province', 'city', 'district',
    'category', 'index_name', 'amount', 'increment', 'rate', 'ratio'
]

insert_statement = 'UPSERT INTO kudu_db.ec_index_month({}) VALUES'.format(','.join(index_columns))

order_sum_columns = [
    'province', 'city', 'cost_time', 'mileage', 'receivable_amount', 'real_amount',
    'pickup_service_fee', 'return_service_fee', 'insurance', 'discount', 'exemption_amount'
]

query_vehicle_count = '''
    select 
      province,city,day,
      count(*) as vehicle_count
    from kudu_db.ec_vehicle_day 
    where year=%(year)s and month=%(month)s and rent_type=0 and operate_status<>1
    group by province, city, day        
'''

query_shop_count = '''
    select 
      province,city,day,
      count(*) as shop_count,
      sum(park_num) as park_sum
    from kudu_db.ec_shop_day 
    where year=%(year)s and month=%(month)s
    group by province, city, day        
'''


def sf(year, month):

    # pd.set_option('use_inf_as_null', True)  # treat None, NaN, INF, -INF as null
    pd.set_option('use_inf_as_na', True)  # treat None, NaN, INF, -INF as null

    with get_impala_connection() as impala_conn:
        _sf_order(year, month, impala_conn)
        _sf_shop(year, month, impala_conn)
        _sf_vehicle(year, month, impala_conn)
        _sf_member(year, month, impala_conn)


def _sf_order(year, month, conn):
    query = """
        select
            vehicle_province as province,
            vehicle_city as city,
            holiday,
            payment_status,
            order_type,
            activity_type,
            star_level,
            cancel_flag,
            cost_time,
            mileage,
            receivable_amount,
            real_amount,
            pickup_service_fee,
            return_service_fee,
            insurance,
            exemption_amount,
            discount,
            illegal_flag
        from kudu_db.ec_order_day
        where year=%(year)s and month=%(month)s
    """

    logger.info('loading order month data of %d-%d...', year, month)
    df = pd.read_sql_query(query, conn, params={'year': year, 'month': month})
    logger.info('finished load data(%d).', len(df))

    values = list()
    if not df.empty:
        df[['province', 'city']] = df[['province', 'city']].fillna('missing')

        logger.info('calculating %s...', index_names[1001])
        g = df[df['payment_status'] == 0].groupby(['province', 'city'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            row = new_row(1001, 1, year, month, r['province'], r['city'], amount=r[0])
            values.append(format_row(row))

        logger.info('calculating %s...', index_names[1002])
        df56 = df[df.payment_status.isin([5, 6])]
        g = df56.groupby(['province', 'city'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            row = new_row(1002, 1, year, month, r['province'], r['city'], amount=r[0])
            values.append(format_row(row))

        logger.info('calculating %s...', index_names[1003])
        g = df.groupby(['province', 'city', 'order_type'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            if r['order_type'] == 0:
                row = new_row(1004, 1, year, month, r['province'], r['city'], amount=r[0])
            elif r['order_type'] == 2:
                row = new_row(1003, 1, year, month, r['province'], r['city'], amount=r[0])
            else:
                row = None

            if row:
                values.append(format_row(row))

        logger.info('calculating %s...', index_names[1005])
        g = df.groupby(['province', 'city', 'activity_type'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            if r['activity_type'] == 2:
                row = new_row(1005, 1, year, month, r['province'], r['city'], amount=r[0])
            elif r['activity_type'] == 3:
                row = new_row(1006, 1, year, month, r['province'], r['city'], amount=r[0])
            else:
                row = None

            if row:
                values.append(format_row(row))

        logger.info('calculating %s...', index_names[1007])
        g = df[df.illegal_flag].groupby(['province', 'city'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            row = new_row(1007, 1, year, month, r['province'], r['city'], amount=r[0])
            values.append(format_row(row))

        logger.info('calculating %s...', index_names[1008])
        g = df56[(df56['activity_type'] == 1) & (df56.order_type.isin([0, 2]))].groupby(['province', 'city'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            row = new_row(1008, 1, year, month, r['province'], r['city'], amount=r[0])
            values.append(format_row(row))

        logger.info('calculating %s...', index_names[1009])
        g = df56.groupby(['province', 'city', 'holiday'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            if r['holiday']:
                row = new_row(1010, 1, year, month, r['province'], r['city'], amount=r[0])
            else:
                row = new_row(1009, 1, year, month, r['province'], r['city'], amount=r[0])

            values.append(format_row(row))

        logger.info('calculating %s...', index_names[1023])
        g = df56.groupby(['province', 'city', 'star_level'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            if r['star_level'] == 1:
                row = new_row(1023, 1, year, month, r['province'], r['city'], amount=r[0])
            elif r['star_level'] == 2:
                row = new_row(1024, 1, year, month, r['province'], r['city'], amount=r[0])
            elif r['star_level'] == 3:
                row = new_row(1025, 1, year, month, r['province'], r['city'], amount=r[0])
            elif r['star_level'] == 4:
                row = new_row(1026, 1, year, month, r['province'], r['city'], amount=r[0])
            elif r['star_level'] == 5:
                row = new_row(1027, 1, year, month, r['province'], r['city'], amount=r[0])
            elif r['star_level'] == 0:
                row = new_row(1028, 1, year, month, r['province'], r['city'], amount=r[0])
            else:
                row = None

            if row:
                values.append(format_row(row))

        logger.info('calculating %s...', index_names[1011])
        df56.fillna(value=np.nan, inplace=True) # fill None(db's null) with NaN
        g = df56[order_sum_columns].groupby(['province', 'city'])
        rs = g.sum().reset_index()
        for i, r in rs.iterrows():
            row = new_row(1011, 1, year, month, r['province'], r['city'], amount=r['cost_time'])
            values.append(format_row(row))
            row = new_row(1012, 1, year, month, r['province'], r['city'], amount=r['mileage'])
            values.append(format_row(row))
            row = new_row(1013, 1, year, month, r['province'], r['city'], amount=r['receivable_amount'])
            values.append(format_row(row))
            row = new_row(1014, 1, year, month, r['province'], r['city'], amount=r['real_amount'])
            values.append(format_row(row))
            row = new_row(1015, 1, year, month, r['province'], r['city'], amount=r['pickup_service_fee'])
            values.append(format_row(row))
            row = new_row(1016, 1, year, month, r['province'], r['city'], amount=r['return_service_fee'])
            values.append(format_row(row))
            row = new_row(1018, 1, year, month, r['province'], r['city'], amount=r['insurance'])
            values.append(format_row(row))
            row = new_row(1020, 1, year, month, r['province'], r['city'], amount=r['discount'])
            values.append(format_row(row))
            row = new_row(1022, 1, year, month, r['province'], r['city'], amount=r['exemption_amount'])
            values.append(format_row(row))

        logger.info('calculating %s...', index_names[1017])
        g = df56[df56['insurance'] > 0].groupby(['province', 'city'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            row = new_row(1017, 1, year, month, r['province'], r['city'], amount=r[0])
            values.append(format_row(row))

        logger.info('calculating %s...', index_names[1019])
        g = df56[df56['discount'] > 0].groupby(['province', 'city'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            row = new_row(1019, 1, year, month, r['province'], r['city'], amount=r[0])
            values.append(format_row(row))

        logger.info('calculating %s...', index_names[1021])
        g = df56[df56['exemption_amount'] > 0].groupby(['province', 'city'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            row = new_row(1021, 1, year, month, r['province'], r['city'], amount=r[0])
            values.append(format_row(row))

    if values:
        with conn.cursor() as cursor:
            statement = '{}{}'.format(insert_statement, ','.join(values))
            cursor.execute(statement)


def _sf_shop(year, month, conn):
    query = """
        select
            province, city, district, day, 
            count(*) as shop_count, 
            sum(park_num) as park_sum,
            sum(success_order_count) as order_sum, 
            sum(income) as income_sum, 
            avg(success_order_count) as order_avg, 
            avg(income) as income_avg, 
            avg(real_income) as real_income_avg
        from kudu_db.ec_shop_day 
        where year=%(year)s and month=%(month)s
        group by province, city, district, day
    """

    logger.info('loading shop month data of %d-%d...', year, month)
    params = {'year': year, 'month': month}
    df = pd.read_sql_query(query, conn, params=params)
    logger.info('finished load data(%d).', len(df))

    values = list()
    if not df.empty:
        df[['province', 'city', 'district']] = df[['province', 'city', 'district']].fillna('missing')

        grouped = df.groupby(['province', 'city', 'district'])

        df1 = grouped['shop_count', 'park_sum'].max().reset_index()
        for i, r in df1.iterrows():
            row = new_row(3001, 3, year, month, r['province'], r['city'], r['district'], amount=r['shop_count'])
            values.append(format_row(row))
            row = new_row(3002, 3, year, month, r['province'], r['city'], r['district'], amount=r['park_sum'])
            values.append(format_row(row))

        df1 = grouped['order_avg', 'income_avg', 'real_income_avg'].mean().reset_index()
        for i, r in df1.iterrows():
            row = new_row(3003, 3, year, month, r['province'], r['city'], r['district'], ratio=r['order_avg'])
            values.append(format_row(row))
            row = new_row(3004, 3, year, month, r['province'], r['city'], r['district'], ratio=r['income_avg'])
            values.append(format_row(row))
            row = new_row(3005, 3, year, month, r['province'], r['city'], r['district'], ratio=r['real_income_avg'])
            values.append(format_row(row))

        df.insert(0, 'i3006', df['order_sum'] / df['park_sum'])
        df.insert(0, 'i3007', df['income_sum'] / df['park_sum'])
        df.insert(0, 'i3009', df['shop_count'] / df['park_sum'])
        # df1.insert(0, 'i3009', (df1['size'] / df1['park_num']).replace(np.inf, 0))
        df1 = df.groupby(['province', 'city', 'district'])['i3006', 'i3007', 'i3009'].mean().reset_index()
        for idx, r in df1.iterrows():
            row = new_row(3006, 3, year, month, r['province'], r['city'], r['district'], ratio=r['i3006'])
            values.append(format_row(row))
            row = new_row(3007, 3, year, month, r['province'], r['city'], r['district'], ratio=r['i3007'])
            values.append(format_row(row))
            row = new_row(3009, 3, year, month, r['province'], r['city'], r['district'], ratio=r['i3009'])
            values.append(format_row(row))

    shop_df = pd.read_sql_query(query_shop_count, conn, params=params)
    shop_df1 = shop_df.groupby(['province', 'city'])['shop_count', 'park_sum'].max().reset_index()

    vehicle_df = pd.read_sql_query(query_vehicle_count, conn, params=params)
    vehicle_df1 = vehicle_df.groupby(['province', 'city'])['vehicle_count'].max().reset_index()
    df = pd.merge(shop_df1, vehicle_df1, how='left', on=['province', 'city']).fillna(0)
    if not df.empty:
        df.insert(0, 'i3008', df['vehicle_count'] / df['shop_count'])
        df.insert(0, 'i3010', df['vehicle_count'] / df['park_sum'])
        df1 = df.groupby(['province', 'city'])['i3008', 'i3010'].mean().reset_index()
        for idx, r in df1.iterrows():
            row = new_row(3008, 3, year, month, r['province'], r['city'], ratio=r['i3008'])
            values.append(format_row(row))
            row = new_row(3010, 3, year, month, r['province'], r['city'], ratio=r['i3010'])
            values.append(format_row(row))

    if values:
        statement = '{}{}'.format(insert_statement, ','.join(values))
        with conn.cursor() as cursor:
            cursor.execute(statement)


def _sf_vehicle(year, month, conn):
    query = """
        select
            province, city, day,
            count(*) as vehicle_count,    
            avg((1440-offline_time-fault_time)/1440) as service_time,            
            sum(if(success_order_count>0, 1, 0)) as service_day,                     
            sum(red_bag_count) as red_bag_sum,                  
            sum(if(fault_time>0, 1, 0)) as fault_count,   
            sum(illegal_count) as illegal_sum,
            sum(success_order_count) as order_sum,
            sum(cost_time) as cost_time_sum,
            sum(mileage) as mileage_sum,
            sum(receivable_amount) as receivable_amount_sum,
            sum(real_amount) as real_amount_sum
        from kudu_db.ec_vehicle_day 
        where year=%(year)s and month=%(month)s and rent_type=0 and operate_status<>1
        group by province, city, day
    """

    logger.info('loading vehicle month data of %d-%d...', year, month)
    params = {'year': year, 'month': month}
    df = pd.read_sql_query(query, conn, params=params)
    logger.info('finished load data(%d).', len(df))

    values = list()
    if not df.empty:
        df[['province', 'city']] = df[['province', 'city']].fillna('missing')

        df.insert(0, 'i4003', (df['service_day'] / df['vehicle_count']))
        df1 = df.groupby(['province', 'city'])['vehicle_count', 'service_time', 'red_bag_sum', 'i4003']\
            .agg({'vehicle_count': 'max', 'red_bag_sum': 'sum', 'i4003': 'mean', 'service_time': 'mean'})\
            .reset_index()
        for i, r in df1.iterrows():
            row = new_row(4001, 4, year, month, r['province'], r['city'], amount=r['vehicle_count'])
            values.append(format_row(row))
            row = new_row(4002, 4, year, month, r['province'], r['city'], ratio=r['service_time'])
            values.append(format_row(row))
            row = new_row(4004, 4, year, month, r['province'], r['city'], amount=r['red_bag_sum'])
            values.append(format_row(row))
            row = new_row(4003, 4, year, month, r['province'], r['city'], ratio=r['i4003'])
            values.append(format_row(row))

        df['order_sum'] = df['order_sum'] / df['vehicle_count']
        df['cost_time_sum'] = df['cost_time_sum'] / df['vehicle_count']
        df['mileage_sum'] = df['mileage_sum'] / df['vehicle_count']
        df['receivable_amount_sum'] = df['receivable_amount_sum'] / df['vehicle_count']
        df['real_amount_sum'] = df['real_amount_sum'] / df['vehicle_count']
        df1 = df.groupby(['province', 'city'])['fault_count', 'illegal_sum'].sum().reset_index()
        for i, r in df1.iterrows():
            row = new_row(4010, 4, year, month, r['province'], r['city'], amount=r['fault_count'])
            values.append(format_row(row))
            row = new_row(4011, 4, year, month, r['province'], r['city'], amount=r['illegal_sum'])
            values.append(format_row(row))

        df1 = df.groupby(['province', 'city'])['order_sum', 'cost_time_sum', 'mileage_sum', 'receivable_amount_sum', 'real_amount_sum']\
            .mean().reset_index()
        for i, r in df1.iterrows():
            row = new_row(4005, 4, year, month, r['province'], r['city'], ratio=r['order_sum'])
            values.append(format_row(row))
            row = new_row(4006, 4, year, month, r['province'], r['city'], ratio=r['cost_time_sum'])
            values.append(format_row(row))
            row = new_row(4007, 4, year, month, r['province'], r['city'], ratio=r['receivable_amount_sum'])
            values.append(format_row(row))
            row = new_row(4008, 4, year, month, r['province'], r['city'], ratio=r['real_amount_sum'])
            values.append(format_row(row))
            row = new_row(4009, 4, year, month, r['province'], r['city'], ratio=r['mileage_sum'])
            values.append(format_row(row))

    if values:
        statement = '{}{}'.format(insert_statement, ','.join(values))
        with conn.cursor() as cursor:
            cursor.execute(statement)


def _sf_member(year, month, conn):
    query = """
        select
            gender,
            birth_date,
            province,
            city,
            review_status,
            active_status,
            leave_status,
            deposit,
            success_order_count
        from kudu_db.ec_member_month
        where year=%(year)s and month=%(month)s
    """

    logger.info('loading member month data of %d-%d...', year, month)
    params = {'year': year, 'month': month}
    with get_impala_connection() as impala_conn:
        df = pd.read_sql_query(query, impala_conn, params=params)
    logger.info('finished load data(%d).', len(df))

    values = list()
    if not df.empty:
        logger.info('filling NaN value...')
        df['birth_date'] = df['birth_date'].fillna('19000101')
        df[['province', 'city']] = df[['province', 'city']].fillna('missing')

        logger.info('calculating member\'s age...')
        df.insert(0, '_age', df['birth_date'].map(calculate_age))

        logger.info('calculating %s...', index_names[2001])
        g = df.groupby(['province', 'city'])
        df_count = g.size().reset_index(name='member_count')
        for i, r in df_count.iterrows():
            row = new_row(2001, 2, year, month, r['province'], r['city'], amount=r['member_count'])
            values.append(format_row(row))

        logger.info('calculating %s...', index_names[2002])
        g = df.groupby(['province', 'city', 'gender'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            if r['gender'] == 0:
                row = new_row(2002, 2, year, month, r['province'], r['city'], amount=r[0])
            elif r['gender'] == 1:
                row = new_row(2003, 2, year, month, r['province'], r['city'], amount=r[0])
            else:
                row = None

            if row:
                values.append(format_row(row))

        logger.info('calculating %s...', index_names[2004])
        g = df.groupby(['province', 'city', '_age'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            if r['_age'] == '20+':
                row = new_row(2004, 2, year, month, r['province'], r['city'], amount=r[0])
            elif r['_age'] == '30+':
                row = new_row(2005, 2, year, month, r['province'], r['city'], amount=r[0])
            elif r['_age'] == '40+':
                row = new_row(2006, 2, year, month, r['province'], r['city'], amount=r[0])
            elif r['_age'] == '50+':
                row = new_row(2007, 2, year, month, r['province'], r['city'], amount=r[0])
            else:
                row = new_row(2008, 2, year, month, r['province'], r['city'], amount=r[0])

            if row:
                values.append(format_row(row))

        logger.info('calculating %s...', index_names[2009])

        review_status = pd.Series(df['review_status'])
        review_status[review_status.isin([1, 4])] = 5  # 1，4状态合并到5（审核未通过）
        review_status[review_status == 3] = 2   # 3状态合并到2（审核通过）
        g = df.groupby(['province', 'city', 'review_status'])
        rs = g.size().reset_index(name='review_count')
        for i, r in rs.iterrows():
            if r['review_status'] == 2:
                row = new_row(2009, 2, year, month, r['province'], r['city'], amount=r['review_count'])
            elif r['review_status'] == 5:
                row = new_row(2010, 2, year, month, r['province'], r['city'], amount=r['review_count'])
            else:
                row = None

            if row:
                values.append(format_row(row))

        logger.info('calculating %s...', index_names[2011])
        g = df[df['deposit'] > 0].groupby(['province', 'city'])
        df_deposit = g.size().reset_index(name='deposit_count')
        for i, r in df_deposit.iterrows():
            row = new_row(2011, 2, year, month, r['province'], r['city'], amount=r['deposit_count'])
            values.append(format_row(row))

        logger.info('calculating %s...', index_names[2018])
        df_review = df[df['review_status'] == 2]\
            .groupby(['province', 'city']).size()\
            .reset_index(name='review_count')
        df1 = pd.merge(df_review, df_deposit, how='left', on=['province', 'city'])
        if not df1.empty:
            df1.insert(0, 'i2018', df1['deposit_count'] / df1['review_count'])
            for idx, r in df1.iterrows():
                row = new_row(2018, 2, year, month, r['province'], r['city'], ratio=r['i2018'])
                values.append(format_row(row))

        logger.info('calculating %s...', index_names[2012])
        active_status = pd.Series(df['active_status'])
        active_status[active_status == 2] = 3   # 2状态合并到3（激活）
        g = df.groupby(['province', 'city', 'active_status'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            if r['active_status'] == 1:
                row = new_row(2013, 2, year, month, r['province'], r['city'], amount=r[0])
            elif r['active_status'] == 3:
                row = new_row(2012, 2, year, month, r['province'], r['city'], amount=r[0])
            else:
                row = None

            if row:
                values.append(format_row(row))

        logger.info('calculating %s...', index_names[2014])
        g = df.groupby(['province', 'city', 'leave_status'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            if r['leave_status'] == 1:
                row = new_row(2014, 2, year, month, r['province'], r['city'], amount=r[0])
            elif r['leave_status'] == 2:
                row = new_row(2015, 2, year, month, r['province'], r['city'], amount=r[0])
            else:
                row = None

            if row:
                values.append(format_row(row))

        logger.info('calculating %s...', index_names[2016])
        success_order_count = pd.Series(df['success_order_count'])
        success_order_count[success_order_count.isin([1, 2])] = 3
        success_order_count[success_order_count > 4] = 4
        g = df.groupby(['province', 'city', 'success_order_count'])
        rs = g.size().reset_index()
        for i, r in rs.iterrows():
            if r['success_order_count'] == 3:
                row = new_row(2016, 2, year, month, r['province'], r['city'], amount=r[0])
            elif r['success_order_count'] == 4:
                row = new_row(2017, 2, year, month, r['province'], r['city'], amount=r[0])
            else:
                row = None

            if row:
                values.append(format_row(row))

    logger.info('calculating %s...', index_names[2019])
    shop_df = pd.read_sql_query(query_shop_count, conn, params=params)
    shop_df1 = shop_df.groupby(['province', 'city'])['shop_count', 'park_sum'].max().reset_index()
    vehicle_df = pd.read_sql_query(query_vehicle_count, conn, params=params)
    vehicle_df1 = vehicle_df.groupby(['province', 'city'])['vehicle_count'].max().reset_index()
    df = pd.merge(df_count, shop_df1, how='left', on=['province', 'city'])
    df = pd.merge(df, vehicle_df1, how='left', on=['province', 'city'])
    if not df.empty:
        df.insert(0, 'i2019', df['member_count'] / df['vehicle_count'])
        df.insert(0, 'i2020', df['member_count'] / df['shop_count'])
        df.insert(0, 'i2021', df['member_count'] / df['park_sum'])
        for idx, r in df.iterrows():
            row = new_row(2019, 2, year, month, r['province'], r['city'], ratio=r['i2019'])
            values.append(format_row(row))
            row = new_row(2020, 2, year, month, r['province'], r['city'], ratio=r['i2020'])
            values.append(format_row(row))
            row = new_row(2021, 2, year, month, r['province'], r['city'], ratio=r['i2021'])
            values.append(format_row(row))

    if values:
        with conn.cursor() as cursor:
            statement = '{}{}'.format(insert_statement, ','.join(values))
            # logger.debug(statement)
            cursor.execute(statement)


def format_row(row):
    return '({})'.format(','.join(map(comm.convert, row.values())))


def new_row(index_id, category, year, month, province=None, city=None, district='missing',
            amount=None, ratio=None):
    row = comm.create_null_row(index_columns)
    row['index_id'] = index_id
    row['category'] = category
    row['index_name'] = index_names.get(index_id)
    row['year'] = year
    row['month'] = month
    row['province'] = province
    row['city'] = city
    row['district'] = district
    if amount is not None:
        row['amount'] = amount if not (np.isnan(amount) or np.isinf(amount)) else None
    if ratio is not None:
        row['ratio'] = ratio if not (np.isnan(ratio) or np.isinf(ratio)) else None
    return row


def calculate_age(birth_date, pattern='YYYYMMDD'):
    birth = arrow.get(birth_date, pattern)
    years = round((arrow.now() - birth).days / 365)
    if (years >= 20) and (years < 30):
        return '20+'
    elif (years >= 30) and (years < 40):
        return '30+'
    elif (years >= 40) and (years < 50):
        return '40+'
    elif (years >= 50) and (years < 60):
        return '50+'
    else:
        return '+++'

