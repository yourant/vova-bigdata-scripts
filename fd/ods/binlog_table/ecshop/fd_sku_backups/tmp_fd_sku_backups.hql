CREATE TABLE IF NOT EXISTS tmp.tmp_fd_ecs_fd_sku_backups(
    value STRING
) 
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/fd_sku_backups';

msck repair table tmp.tmp_fd_ecs_fd_sku_backups;
