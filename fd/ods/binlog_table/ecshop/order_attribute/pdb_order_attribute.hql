CREATE EXTERNAL TABLE IF NOT EXISTS pdb.fd_ecshop_order_attribute(
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/order_attribute';

MSCK REPAIR TABLE pdb.fd_ecshop_order_attribute;
