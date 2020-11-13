CREATE TABLE IF NOT EXISTS tmp.tmp_fd_ecs_region(
    value STRING
) 
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/ecs_region';

MSCK REPAIR TABLE tmp.tmp_fd_ecs_region;
