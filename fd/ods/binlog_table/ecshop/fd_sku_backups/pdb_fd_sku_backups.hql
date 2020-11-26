CREATE EXTERNAL TABLE IF NOT EXISTS pdb.fd_ecshop_fd_sku_backups(
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/fd_sku_backups';

msck repair table pdb.fd_ecshop_fd_sku_backups;
