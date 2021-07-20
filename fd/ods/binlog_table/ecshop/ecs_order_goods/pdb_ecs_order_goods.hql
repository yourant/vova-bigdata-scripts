CREATE EXTERNAL TABLE IF NOT EXISTS pdb.fd_ecshop_ecs_order_goods(
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/ecs_order_goods'
MSCK REPAIR TABLE pdb.fd_ecshop_ecs_order_goods;
