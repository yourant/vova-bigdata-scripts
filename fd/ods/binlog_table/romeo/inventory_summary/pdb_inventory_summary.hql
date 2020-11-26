CREATE TABLE IF NOT EXISTS pdb.fd_romeo_inventory_summary(
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/inventory_summary';

MSCK REPAIR TABLE pdb.fd_romeo_inventory_summary;
