CREATE EXTERNAL TABLE IF NOT EXISTS pdb.fd_ecshop_ecs_region(
    value STRING
) 
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/ecs_region';

MSCK REPAIR TABLE pdb.fd_ecshop_ecs_region;
