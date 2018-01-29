# -*- coding: utf-8 -*-

import logging
import hashlib
import re
import os
import arrow

from .config import get_config
from .datasource import get_impala_connection
from . import common as comm

logger = logging.getLogger(__name__)

user_config = get_config()
desen_config = user_config['desensitization']

id_card_pattern = r'^([1-9]\d{5}[12]\d{3}(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])\d{3}[0-9xX])$'

USER_CONFIG_PATH = os.path.expanduser('~/.evcard')

encrypt_columns = list()
convert_columns = list()

desen_version = int(arrow.now().format('YYMMDDHHmm'))


def goo(ddl):
    global encrypt_columns
    global convert_columns

    import kudu

    kudu_host = user_config.get('kudu').get('host')
    kudu_port = user_config.get('kudu').get('port', 7051)
    kudu_client = kudu.connect(host=kudu_host, port=kudu_port)

    summary = {}
    tables = desen_config.get('tables')
    for table in tables:
        logger.info('copying %s...', table)

        table_config = desen_config.get(table)
        primary_key = table_config.get('primary_key', 'uuid') if table_config else 'uuid'
        exclude_columns = table_config.get('exclude_columns', []) if table_config else []

        schema = _parse_table(table, primary_key, exclude_columns)
        columns = schema[0]

        if ddl:
            print(columns)
            logger.info(schema[1])
            continue

        encrypt_columns = table_config.get('encrypt_columns', []) if table_config else []
        convert_columns = table_config.get('convert_columns', []) if table_config else []

        if encrypt_columns or convert_columns:
            count = _copy_to_kudu(kudu_client, table, primary_key, columns)
            summary[table] = count
        else:
            from_columns = '{},{} as desen_version'.format(','.join(columns), desen_version)
            to_columns = '{},desen_version'.format(','.join(columns))
            if primary_key == 'uuid':
                from_columns = 'uuid(),{}'.format(from_columns)
                to_columns = '{},{}'.format(primary_key, to_columns)

            source_table = table + '_' + desen_config.get('source_suffix') if desen_config.get('source_suffix') else table
            target_table = table + '_' + desen_config.get('target_suffix') if desen_config.get('target_suffix') else table

            copy_statement = 'UPSERT INTO {}.{}({}) SELECT {} FROM {}.{}'.format(
                desen_config.get('target_database'), target_table, to_columns,
                from_columns, desen_config.get('source_database'), source_table
            )
            logger.debug(copy_statement)

            with get_impala_connection() as impala_conn:
                with impala_conn.cursor() as cursor:
                    cursor.execute(copy_statement)

        logger.info('%s(version=%d) finished.', table, desen_version)


def _copy_to_kudu(kudu_client, table_name, primary_key, columns):
    if desen_config.get('target_suffix'):
        table = kudu_client.table('impala::{}.{}_{}'.format(desen_config.get('target_database'), table_name, desen_config.get('target_suffix')))
    else:
        table = kudu_client.table('impala::{}.{}'.format(desen_config.get('target_database'), table_name))

    session = kudu_client.new_session()

    copy_columns = columns.copy() + [col for col in convert_columns if col not in columns]
    if primary_key == 'uuid':
        copy_columns.append('uuid() as uuid')

    source_table = table_name + '_' + desen_config.get('source_suffix') if desen_config.get('source_suffix') else table_name
    query_statement = 'SELECT {} FROM {}.{}'.format(','.join(copy_columns), desen_config.get('source_database'), source_table)
    logger.debug(query_statement)

    with get_impala_connection() as impala_conn:
        with impala_conn.cursor() as cursor:
            batch_size = user_config.get('batch_size')
            cursor.arraysize = batch_size

            cursor.execute(query_statement)
            _columns = [metadata[0] for metadata in cursor.description]

            _page = 1
            total_count = 0
            df = comm.as_grid(cursor, _columns, batch_size)
            while len(df) > 0:
                total_count += len(df)
                logger.debug('%s page %s(%s)...', table_name, _page, total_count)

                for row in df:
                    for col in encrypt_columns:
                        row[col] = _md5(row.get(col))
                    for col in convert_columns:
                        _woo(table_name, row, col)

                    row['desen_version'] = desen_version

                    op = table.new_upsert(row)
                    session.apply(op)

                try:
                    session.flush()
                except kudu.KuduBadStatus as e:
                    print(session.get_pending_errors())

                df = comm.as_grid(cursor, _columns, batch_size)
                _page += 1

    return total_count


def _woo(table_name, target, column):
    value = target.get(column)

    if table_name == 'membership_info' and column == 'driver_code':
        id_card = _parse_id_card(value)
        if id_card[0]:
            target['birth_date'] = id_card[0]
        if id_card[1]:
            target['gender'] = id_card[1]
        del target[column]

    if table_name == 'vehicle_info' and column == 'vehicle_no':
        if value and len(value) > 1:
            target[column] = value[0:2] + '*'*(len(value) - 2)


def _md5(content):
    if content is None:
        return None
    else:
        str_value = content if isinstance(content, str) else str(content)
        md5 = hashlib.md5()
        md5.update(str_value.encode('utf-8'))
        return md5.hexdigest()


def _parse_id_card(id_card):
    if id_card and re.match(id_card_pattern, id_card):
        return id_card[6:14], '1' if int(id_card[16]) % 2 == 0 else '0'
    else:
        return None, None


def _parse_table(table, primary_key, exclude_columns=[]):
    source_table = table + '_' + desen_config.get('source_suffix') if desen_config.get('source_suffix') else table
    _statement = 'SELECT * FROM {}.{} WHERE 1=0'.format(desen_config.get('source_database'), source_table)

    target_table = table + '_' + desen_config.get('target_suffix') if desen_config.get('target_suffix') else table

    _columns = list()
    if primary_key == 'uuid':
        _columns.append('uuid STRING NOT NULL,')

    with get_impala_connection() as impala_conn:
        with impala_conn.cursor() as cursor:
            cursor.execute(_statement)
            origin_columns = [(m[0], m[1]) for m in cursor.description if m[0] not in exclude_columns]
            for col in origin_columns:
                if col[0] == primary_key:
                    _columns.append('{} {} NOT NULL,'.format(col[0], col[1]))
                else:
                    _columns.append('{} {},'.format(col[0], col[1]))

    create_statement = '''
CREATE TABLE {db}.{table} (
{cols}
desen_version INT NOT NULL DEFAULT 0,
PRIMARY KEY({pk})
)
PARTITION BY HASH ({pk}) PARTITIONS 10
STORED AS KUDU;
    '''.format(
        db=desen_config.get('target_database'),
        table=target_table,
        cols='\n'.join(_columns),
        pk=primary_key
    )

    return [col[0] for col in origin_columns], create_statement

