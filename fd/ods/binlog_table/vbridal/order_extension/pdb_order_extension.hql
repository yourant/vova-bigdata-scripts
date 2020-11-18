CREATE EXTERNAL TABLE IF NOT EXISTS pdb.fd_vb_order_extension (
    value STRING
) COMMENT 'kafka同步过来的数据库订单扩展临时表'
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/order_extension';

MSCK REPAIR TABLE pdb.fd_vb_order_extension;
