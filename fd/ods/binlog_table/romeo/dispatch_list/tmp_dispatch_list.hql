CREATE TABLE IF NOT EXISTS tmp.tmp_fd_romeo_dispatch_list(
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/dispatch_list';

MSCK REPAIR TABLE tmp.tmp_fd_romeo_dispatch_list;
