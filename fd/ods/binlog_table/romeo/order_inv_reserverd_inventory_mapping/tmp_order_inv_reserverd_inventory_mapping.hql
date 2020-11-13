CREATE TABLE IF NOT EXISTS tmp.tmp_fd_romeo_order_inv_reserverd_inventory_mapping(
    value STRING
) 
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION 's3a://vova-bd-test/flume/fd/erp/order_inv_reserverd_inventory_mapping';

MSCK REPAIR TABLE tmp.tmp_fd_romeo_order_inv_reserverd_inventory_mapping;
