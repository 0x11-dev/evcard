# -*- coding: utf-8 -*-

import json

G_DATE_FORMATTER = 'YYYYMMDDHHmmssSSS'
G_BATCH_SIZE = 500

HOLIDAYS = (
    '20140101', '20140131', '20140203', '20140204', '20140205', '20140206', '20140407', '20140501',
    '20140502', '20140602', '20140908', '20141001', '20141002', '20141003', '20141006', '20141007',
    '20150101', '20150102', '20150218', '20150219', '20150220', '20150223', '20150224', '20150406',
    '20150501', '20150622', '20151001', '20151002', '20151005', '20151006', '20151007',
    '20160101', '20160208', '20160209', '20160210', '20160211', '20160212', '20160404', '20160502',
    '20160609', '20160610', '20160915', '20160916', '20161003', '20161004', '20161005', '20161006',
    '20161007',
    '20170102', '20170127', '20170130', '20170131', '20170201', '20170202', '20170403', '20170404',
    '20170501', '20170529', '20170530', '20171002', '20171003', '20171004', '20171005', '20171006',
)


def convert(value):
    return 'null' if value is None else "'{}'".format(value) if isinstance(value, str) else str(value)


def create_null_row(columns):
    null_row = dict()
    for col_name in columns:
        null_row[col_name] = None
    return null_row


def init_row(columns):
    _row = dict()
    for col_name, default_value in columns.items():
        _row[col_name] = default_value
    return _row


def as_grid(cursor, columns, size=G_BATCH_SIZE):

    if size <= 0:
        results = cursor.fetchall()
    elif size == 1:
        results = cursor.fetchone()  # tuple
    else:
        results = cursor.fetchmany(size)

    if size == 1:
        new_row = dict()
        if results:
            for idx in range(0, len(columns)):
                new_row[columns[idx]] = results[idx]

        return new_row
    else:
        grid = list()
        for rs in results:
            new_row = dict()
            for idx in range(0, len(columns)):
                new_row[columns[idx]] = rs[idx]
            grid.append(new_row)
        return grid


def to_formatted_dict(obj):
    return json.dumps(obj, indent=2)


def get_province_city(longitude, latitude):
    import requests
    import json

    # 经纬度获取城市
    baidu_url = 'http://api.map.baidu.com/geocoder/v2/?ak=D27068972f0f7c39a1d586eb363541f4&callback=renderReverse&location={},{}&output=json&pois=0'.format(latitude, longitude)
    req = requests.get(baidu_url)
    content = req.text
    content = content.replace("renderReverse&&renderReverse(", "")
    content = content[:-1]
    baidu_addr = json.loads(content)
    province = baidu_addr["result"]["addressComponent"]["province"]
    city = baidu_addr["result"]["addressComponent"]["city"]
    district = baidu_addr["result"]["addressComponent"]["district"]

    return province, city

'''

cur.execute('select shop_name,address,longitude,latitude,org_id from evcard_tmp.shop_info')
rs_shop = cur.fetchall()
orgs = list()
for row in rs_shop:
    org_id = row[4]
    if org_id in orgs:
        continue
    else:
        lng='{}.{}'.format(str(row[2])[0:3],str(row[2])[3:])
        lat='{}.{}'.format(str(row[3])[0:2],str(row[3])[2:])
        loc = get_province_city(lng, lat)
        if loc and loc[0]:
            print(org_id, row[1], loc)
            cur.execute('update kudu_db.ec_org_info set province=%(p)s,city=%(c)s where org_id=%(o)s',{'p':loc[0],'c':loc[1],'o':org_id})
            orgs.append(org_id)
            time.sleep(.200)
        
'''
