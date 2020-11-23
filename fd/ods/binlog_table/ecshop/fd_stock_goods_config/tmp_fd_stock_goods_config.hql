CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_fd_ecs_fd_stock_goods_config(
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/fd_stock_goods_config';

msck repair table tmp.tmp_fd_ecs_fd_stock_goods_config;
