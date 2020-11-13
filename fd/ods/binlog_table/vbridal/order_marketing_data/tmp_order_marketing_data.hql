CREATE TABLE IF NOT EXISTS tmp.tmp_fd_order_marketing_data (
    value STRING
) COMMENT 'kafka同步过来的数据库订单session关联临时表'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/order_marketing_data';


MSCK REPAIR TABLE tmp.tmp_fd_order_marketing_data;
