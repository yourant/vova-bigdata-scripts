CREATE TABLE IF NOT EXISTS pdb.fd_ecshop_ecs_region(
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/ecs_region';

MSCK REPAIR TABLE pdb.fd_ecshop_ecs_region;
