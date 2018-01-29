# -*- coding: utf-8 -*-


import redis
from impala.dbapi import connect
from .config import get_config

user_config = get_config()

redis_config = user_config['redis']
redis_pool = redis.ConnectionPool(host=redis_config['host'],
                                  port=redis_config['port'],
                                  decode_responses=redis_config['decode_responses'])   # host是redis主机，需要redis服务端和客户端都起着 redis默认端口是6379
# rdb = redis.ConnectionPool(connection_class=redis.UnixDomainSocketConnection, path="/var/run/redis/redis.sock")

impala_config = user_config['impala']
# kudu_conn = connect(host='10.20.140.11',port=21050,auth_mechanism='PLAIN',user='duba',password='duba@2017')


def get_impala_connection():
    return connect(host=impala_config['host'],
                   port=impala_config['port'],
                   auth_mechanism='PLAIN',
                   user=impala_config['user'],
                   password=impala_config['password'])


def get_redis_connection():
    return redis.StrictRedis(connection_pool=redis_pool)
