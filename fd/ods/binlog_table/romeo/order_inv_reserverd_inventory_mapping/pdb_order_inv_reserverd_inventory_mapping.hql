CREATE EXTERNAL TABLE IF NOT EXISTS pdb.pdb_fd_order_inv_reserverd_inventory_mapping(
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION 's3a://bigdata-offline/warehouse/pdb/fd/romeo/order_inv_reserverd_inventory_mapping';

MSCK REPAIR TABLE pdb.pdb_fd_order_inv_reserverd_inventory_mapping;
