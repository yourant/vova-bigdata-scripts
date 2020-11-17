CREATE TABLE IF NOT EXISTS tmp.tmp_fd_order_info(
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/order_info';

MSCK REPAIR TABLE tmp.tmp_fd_order_info;
