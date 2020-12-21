CREATE TABLE IF NOT EXISTS pdb.fd_romeo_inventory_item_detail(
    value STRING
) 
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/inventory_item_detail';

MSCK REPAIR TABLE pdb.fd_romeo_inventory_item_detail;
