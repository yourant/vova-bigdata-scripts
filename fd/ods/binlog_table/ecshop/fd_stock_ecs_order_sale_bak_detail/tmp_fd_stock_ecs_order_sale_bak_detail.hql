CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_fd_ecs_fd_stock_ecs_order_sale_bak_detail
( 
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/fd_stock_ecs_order_sale_bak_detail';

msck repair table tmp.tmp_fd_ecs_fd_stock_ecs_order_sale_bak_detail;
