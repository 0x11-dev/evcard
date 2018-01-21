### IMPALA DATA TYPE ###
# TINYINT, SMALLINT, INT, BIGINT
# DOUBLE, FLOAT
# DECIMAL, REAL
# CHAR, STRING, VARCHAR
# TIMESTAMP
# BOOLEAN



### KUDU DATA TYPE ###
# int8, int16, int32
# int64, unixtime_micros
# float, double
# bool
# string, binary


CREATE TABLE ec_org_info (
  org_id STRING,
  org_name STRING,
  org_class tinyint,  
  province_name STRING,
  province_code STRING,
  city_name STRING,
  city_code STRING,
  PRIMARY KEY(org_id)
)
PARTITION BY HASH(org_id) PARTITIONS 4
STORED AS KUDU;

CREATE TABLE ec_order_day (
  order_seq string,
  year int,
  month int,
  order_time string,
  week_day tinyint,
  holiday boolean,
  auth_id string,
  org_id string,
  vin string,
  vehicle_province string,
  vehicle_city string,
  payment_status tinyint,
  order_type tinyint,
  activity_type tinyint,
  market_activity_type tinyint,
  pickup_store_seq bigint,
  pickup_province string,
  pickup_city string,
  pickup_district string,
  return_store_seq bigint,
  return_province string,
  return_city string,
  return_district string,
  star_level tinyint,
  cancel_flag tinyint,
  org_free boolean,
  cost_time bigint,
  mileage int,
  pay_way tinyint,
  receivable_amount double,
  real_amount double,
  pickup_service_fee double,
  return_service_fee double,
  insurance double,
  exemption_amount double,
  discount double,
  discount_rate double,
  illegal_flag boolean,
  PRIMARY KEY(order_seq,year,month)
)
PARTITION BY HASH (order_seq) PARTITIONS 4,
RANGE (year)
(
  PARTITION VALUES < 2017,
  PARTITION VALUE = 2017,
  PARTITION VALUE = 2018
)
STORED AS KUDU;


CREATE TABLE ec_vehicle_day (
  vin string COMMENT '车架号',
  year int,
  month int,
  day int,
  vehicle_model_seq bigint COMMENT '车型号',
  vehicle_model_name string COMMENT '车型名称',
  org_id string COMMENT '购买方',
  province string COMMENT '省份',
  city string COMMENT '城市',
  rent_type tinyint COMMENT '车辆租赁类型',
  operate_status tinyint COMMENT '运营状态',
  illegal_count int DEFAULT 0 COMMENT '违章次数',
  fault_time int DEFAULT 0 COMMENT '故障时间（分钟）',
  offline_time int DEFAULT 0 COMMENT '下线时间（分钟）',
  online_rate double DEFAULT 100 COMMENT '上线率',
  cancel_order_count int DEFAULT 0 COMMENT '取消订单数',
  success_order_count int DEFAULT 0 COMMENT '成功订单数',
  cost_time bigint DEFAULT 0 COMMENT '时长',
  mileage bigint DEFAULT 0 COMMENT '距离',
  receivable_amount double COMMENT '应收收入',
  real_amount double DEFAULT 0 COMMENT '实际收入',
  red_bag_count int DEFAULT 0 COMMENT '红包车次数',
  star_one int DEFAULT 0 COMMENT '1星评价数',
  star_two int DEFAULT 0 COMMENT '2星评价数',
  star_three int DEFAULT 0 COMMENT '3星评价数',
  star_four int DEFAULT 0 COMMENT '4星评价数',
  star_five int DEFAULT 0 COMMENT '5星评价数',
  star_zero int DEFAULT 0 COMMENT '0星评价数',
  PRIMARY KEY(vin,year,month,day)
)
PARTITION BY HASH (vin) PARTITIONS 4,
RANGE (year)
(
  PARTITION VALUES < 2017,
  PARTITION VALUE = 2017,
  PARTITION VALUE = 2018
)
STORED AS KUDU;

CREATE TABLE ec_shop_day (
  shop_seq bigint COMMENT '网点编号',
  year int,
  month int,
  day int,
  province string COMMENT '省份',
  city string COMMENT '城市',
  district string COMMENT '行政区',
  shop_type tinyint COMMENT '网点类型',
  shop_status tinyint COMMENT '网点状态',
  park_num int COMMENT '泊位数',
  stack_num int COMMENT '充电桩数',
  cancel_order_count double COMMENT '取消订单数',
  success_order_count double COMMENT '成功订单数',
  pickup_num int COMMENT '取车数',
  return_num int COMMENT '还车数',
  income double COMMENT '网点收入',
  real_income double COMMENT '网点实际收入',
  turnover double COMMENT '泊位周转数',
  park_income double COMMENT '泊位收入',
  park_real_income double COMMENT '泊位实际收入',
  PRIMARY KEY(shop_seq, year, month, day)
)
PARTITION BY HASH (shop_seq) PARTITIONS 4,
RANGE (year)
(
  PARTITION VALUES < 2017,
  PARTITION VALUE = 2017,
  PARTITION VALUE = 2018
)
STORED AS KUDU;

CREATE TABLE ec_member_month (
  auth_id string COMMENT '会员ID',
  year int,
  month int,
  province string COMMENT '省份',
  city string COMMENT '城市',
  gender tinyint COMMENT '性别',
  birth_date string COMMENT '出生年月',
  review_status tinyint COMMENT '审核状态',
  active_status tinyint COMMENT '活跃状态',
  leave_status tinyint COMMENT '流失状态',
  deposit double COMMENT '押金',
  cancel_order_count int DEFAULT 0 COMMENT '取消订单数',
  man_cancel_count int DEFAULT 0 COMMENT '主动取消订单数',
  sys_cancel_count int DEFAULT 0 COMMENT '系统取消订单数',
  success_order_count int DEFAULT 0 COMMENT '成功订单数',
  cost_time int DEFAULT 0 COMMENT '时长',
  mileage int DEFAULT 0 COMMENT '距离',
  receivable_amount double COMMENT '应收收入',
  real_amount double DEFAULT 0 COMMENT '实际收入',
  pickup_num int DEFAULT 0 COMMENT '取车数',
  pickup_service_fee double DEFAULT 0 COMMENT '取车费用',
  return_num int DEFAULT 0 COMMENT '还车数',
  return_service_fee double DEFAULT 0 COMMENT '还车费用',
  insurance_num int DEFAULT 0 COMMENT '不计免赔次数',
  insurance double DEFAULT 0 COMMENT '不计免赔金额',
  discount_num int DEFAULT 0 COMMENT '优惠次数',
  discount double DEFAULT 0 COMMENT '优惠金额',
  exemption_num int DEFAULT 0 COMMENT '减免次数',
  exemption_amount double DEFAULT 0 COMMENT '减免金额',
  illegal_num int DEFAULT 0 COMMENT '违章次数',
  PRIMARY KEY(auth_id, year, month)
)
PARTITION BY HASH (auth_id) PARTITIONS 4,
RANGE (year)
(
  PARTITION VALUES < 2017,
  PARTITION VALUE = 2017,
  PARTITION VALUE = 2018
)
STORED AS KUDU;

CREATE TABLE ec_index_month (
  index_id INT COMMENT '指标ID',
  year int,
  month int,
  province string COMMENT '省份',
  city string COMMENT '城市',
  district string DEFAULT 'missing' COMMENT '行政区',
  category tinyint COMMENT '指标类(1:订单,2:会员,3:网点,4:车辆)',
  index_name string COMMENT '指标名称',
  amount DOUBLE COMMENT '总量',
  increment DOUBLE COMMENT '增量',
  rate DOUBLE COMMENT '增长率',
  ratio DOUBLE COMMENT '比值',
  PRIMARY KEY(index_id,year,month,province,city,district)
)
PARTITION BY HASH (index_id) PARTITIONS 4,
RANGE (year)
(
  PARTITION VALUES < 2017,
  PARTITION VALUE = 2017,
  PARTITION VALUE = 2018
)
STORED AS KUDU;
