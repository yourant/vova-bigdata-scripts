CREATE TABLE IF NOT EXISTS tmp.tmp_fd_ecs_order_attribute(
    value STRING
) 
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/order_attribute';

MSCK REPAIR TABLE tmp.tmp_fd_ecs_order_attribute;
