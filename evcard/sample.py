# -*- coding: utf-8 -*-

import logging
import arrow

from .config import get_config
from .datasource import get_impala_connection

logger = logging.getLogger(__name__)

desen_version = int(arrow.now().format('YYMMDDHHmm'))

user_config = get_config()
sample_config = user_config['sample']


def go_sample(from_date=None, to_date=None):
    date_range = sample_config.get('date_range')
    if from_date:
        date_range['begin_date'] = from_date
    if to_date:
        date_range['end_date'] = to_date

    begin_date = arrow.get(date_range.get('begin_date'), 'YYYYMMDD')
    end_date = arrow.get(date_range.get('end_date'), 'YYYYMMDD')
    date_range['begin_date_l'] = begin_date.format('YYYY-MM-DD')
    date_range['end_date_l'] = end_date.format('YYYY-MM-DD')
    print(date_range)

    tables = sample_config.get('tables')
    for table in tables:
        logger.info('copying %s...', table)

        columns = _parse_table(table)
        conditions = sample_config.get(table)

        from_columns = '{},{} as desen_version'.format(','.join(columns), desen_version)
        to_columns = '{},desen_version'.format(','.join(columns))

        query_statement = 'SELECT {} FROM {}.{}'.format(from_columns, sample_config.get('source_database'), table)
        if conditions:
            query_statement = '{} WHERE {}'.format(query_statement, conditions)

        copy_statement = 'UPSERT INTO {}.{}({}) {}'.format(sample_config.get('target_database'), table, to_columns, query_statement)
        # logger.debug(copy_statement)
        _execute(table, copy_statement, date_range)
        logger.info('%s(version=%d) finished.', table, desen_version)

    if 'order_info' in tables:
        copy_membership_info()
        copy_agency_info()


def copy_membership_info():
    logger.info('copying membership_info...')
    columns = _parse_table('membership_info')

    from_columns = '{},{} as desen_version'.format(','.join(columns), desen_version)
    to_columns = '{},desen_version'.format(','.join(columns))

    query_auth_id = 'SELECT distinct(auth_id) FROM {}.order_info'.format(sample_config.get('target_database'))
    query_membership = 'SELECT {} FROM {}.membership_info WHERE auth_id IN ({})'.format(
        from_columns,
        sample_config.get('source_database'),
        query_auth_id)

    copy_statement = 'UPSERT INTO {}.membership_info({}) {}'.format(
        sample_config.get('target_database'),
        to_columns,
        query_membership)

    # logger.debug(copy_statement)
    _execute('membership_info', copy_statement)
    logger.info('membership_info(version=%d) finished.', desen_version)


def copy_agency_info():
    logger.info('copying agency_info...')

    columns = _parse_table('agency_info')

    from_columns = '{},{} as desen_version'.format(','.join(columns), desen_version)
    to_columns = '{},desen_version'.format(','.join(columns))

    query_org_id = 'SELECT distinct(org_id) FROM {}.order_info'.format(sample_config.get('target_database'))
    query_agency = 'SELECT {} FROM {}.agency_info WHERE agency_id IN ({})'.format(
        from_columns,
        sample_config.get('source_database'),
        query_org_id)

    copy_statement = 'UPSERT INTO {}.agency_info({}) {}'.format(
        sample_config.get('target_database'),
        to_columns,
        query_agency)

    # logger.debug(copy_statement)
    _execute('agency_info', copy_statement)
    logger.info('agency_info(version=%d) finished.', desen_version)


def _execute(table, statement, parameters=None):
    with get_impala_connection() as impala_conn:
        with impala_conn.cursor() as cursor:
            cursor.execute(statement, parameters)

            delete_statement = 'DELETE FROM {}.{} WHERE desen_version<>{}'.format(
                sample_config.get('target_database'),
                table,
                desen_version)

            cursor.execute(delete_statement)


def _parse_table(table):
    _statement = 'SELECT * FROM {}.{} WHERE 1=0'.format(sample_config.get('source_database'), table)

    with get_impala_connection() as impala_conn:
        with impala_conn.cursor() as cursor:
            cursor.execute(_statement)
            origin_columns = [m[0] for m in cursor.description]

    origin_columns.remove('desen_version')
    return origin_columns
