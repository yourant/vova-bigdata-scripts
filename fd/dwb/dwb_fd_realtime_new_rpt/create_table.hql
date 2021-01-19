CREATE TABLE if not exists dwb.dwb_fd_realtime_new_rpt (
project                             string COMMENT 'd_组织',
platform                            string COMMENT 'd_平台',
country                             string COMMENT 'd_国家',
hour                                string COMMENT 'd_小时',
order_number                        bigint COMMENT 'd_订单数',
gmv                                 decimal(15,4) COMMENT 'd_gmv',
session_number                      bigint COMMENT 'd_session数',
conversion_rate                     decimal(15,4) COMMENT 'd_整体转化率'
)partitioned by(pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS parquet
TBLPROPERTIES ("parquet.compress"="SNAPPY");
