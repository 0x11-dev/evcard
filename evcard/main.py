
# -*- coding: utf-8 -*-

"""
Main entry point for the `evcard` command.

The code in this module is also a good example of how to use evcard as a
library rather than a script.
"""
import os
import signal
import logging

from .datasource import get_impala_connection
from pandas import DataFrame
import pyarrow.parquet as pq
import pyarrow as pa

from evcard.lib import RdsModelProtobuf, TbField, TbByteString
from confluent_kafka import Consumer, KafkaError
from .config import get_config

logger = logging.getLogger(__name__)

terminate = False


def signal_handler(signum, frame):
    global terminate
    terminate = True


def subscribe():
    kafka_config = get_config()['kafka']

    signal.signal(signal.SIGINT, signal_handler)

    logger.info('Subscribe message from kafka: topic={}...'.format(kafka_config['topic']))

    conf = {
        'bootstrap.servers': ','.join(kafka_config['servers']),
        # 'group.id': kafka_config['group'],
        'group.id': 'TEST_0002',
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'api.version.request': True,
    }

    consumer = Consumer(**conf)

    consumer.subscribe([kafka_config['topic']])

    tables = []

    summary = {}

    try:
        running = True
        count = 0
        while running:
            msg = consumer.poll(5000)
            if msg is None:
                print('Game over!', summary)
                continue

            if not msg.error():
                oo = RdsModelProtobuf()
                oo.ParseFromString(msg.value())

                table_name = oo.key.split('|', 1)[0].split('_', 1)[1]
                # logger.info('Received message : {}-{} -> {}|{}'.format(msg.partition(), msg.offset(), oo.opt, table_name))

                count += 1
                if count % 1000 == 0:
                    logger.info('%d: %s@%d-%d', count, table_name, msg.partition(), msg.offset())
                    # print('===>', count, table_name, msg.timestamp(), msg.partition(), msg.offset())
                    print(summary)

                if oo.opt == 'UPDATE':
                    key = 'U:{}'.format(table_name)
                    summary[key] = summary.get(key, 0) + 1
                    pass
                    # update_message(table_name, oo)
                    # running = False
                elif oo.opt == 'DELETE':
                    key = 'D:{}'.format(table_name)
                    summary[key] = summary.get(key, 0) + 1
                    pass
                    # delete_message(table_name, oo)
                    # running = False
                elif oo.opt == 'INSERT':
                    key = 'I:{}'.format(table_name)
                    summary[key] = summary.get(key, 0) + 1
                    # if table_name in tables:
                    #     pass
                    # else:
                    #     tables.append(table_name)
                    #     print(oo)
                    #     insert_message(table_name, oo)
                        # running = False
                else:
                    pass
                    # insert_message(table_name, oo)

                # if oo.key == 'siac_membership_info|INSERT':
                #     print(oo)
                #     insert_message(table_name, oo)
                #     running = False

            elif msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error(msg.error())
                running = False

            if terminate:
                logger.info('Aborted by user.')
                running = False

    except Exception as ex:
        logger.error(ex)
    finally:
        consumer.close()


def insert_message(table_name, msg):
    fields = []
    values = []

    for field in msg.fieldList:
        value = field.tbByteString
        if value.len == 0:
            continue

        fields.append(field.fieldname)
        values.append(field_decode(field, value))

    statement = list(['UPSERT INTO {}('.format(table_name)])
    statement.append('{})'.format(','.join(fields)))
    statement.append(' VALUES({})'.format(','.join(values)))

    full_statement = ''.join(statement)
    logger.info(full_statement)


def update_message(table_name, msg):
    wheres = []
    values = []
    fields = {}
    keys = msg.primaryKeys.lower().split(',')

    for field in msg.fieldList:
        value = field.tbByteString

        if field.fieldname in fields:
            if keys.count(field.fieldname) > 0:
                wheres.append('{}={}'.format(field.fieldname, fields[field.fieldname]))
            else:
                new_value = field_decode(field, value)
                if fields[field.fieldname] != new_value:
                    if new_value:
                        values.append('{}={}'.format(field.fieldname, new_value))
                    else:
                        values.append('{}=null'.format(field.fieldname))
        else:
            fields[field.fieldname] = field_decode(field, value)

    statement = list(['UPDATE {} SET '.format(table_name)])
    statement.append(','.join(values))
    statement.append(' WHERE {}'.format(' AND '.join(wheres)))

    full_statement = ''.join(statement)
    logger.info(full_statement)


def delete_message(table_name, msg):
    wheres = []
    keys = msg.primaryKeys.lower().split(',')

    for field in msg.fieldList:
        value = field.tbByteString
        if keys.count(field.fieldname) > 0:
            wheres.append('{}={}'.format(field.fieldname, field_decode(field, value)))

    full_statement = 'DELETE FROM {} WHERE {}'.format(table_name, ' AND '.join(wheres))
    logger.info(full_statement)


def field_decode(field, value):
    if value.len == 0:
        return None

    if field.type in ['STRING', 'TIMESTAMP', 'DATETIME', 'DATE']:
        return "'{}'".format(value.bytes.decode('utf-8').strip())
        # return "'{}'".format(value.bytes.decode(field.encoding).strip())
    else:
        return value.bytes.decode().strip()


def as_pandas(cursor, names):
    return DataFrame.from_records(cursor.fetchmany(), columns=names)


def parquet():

    query = '''
        select * from evcard_tmp.order_info where strleft(created_time,4)='2015' limit 10
    '''

    with get_impala_connection() as impala_conn:
        with impala_conn.cursor() as cursor:
            cursor.arraysize = 500

            cursor.execute(query)
            _columns = [metadata[0] for metadata in cursor.description]

            writer = pq.ParquetWriter('city.1.parquet', table.schema)

            _page = 1
            total_count = 0
            df = as_pandas(cursor, _columns)
            while len(df) > 0:
                total_count += len(df)
                logger.debug('process page %s(%s)...', _page, total_count)

                table = pa.Table.from_pandas(df, preserve_index=False)
                writer.write_table(table)

                df = as_pandas(cursor, _columns)
                _page += 1
