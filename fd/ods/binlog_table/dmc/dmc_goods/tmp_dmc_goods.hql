CREATE TABLE IF NOT EXISTS tmp.tmp_fd_dmc_goods (
    value STRING
) 
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/dmc_goods';

msck repair table tmp.tmp_fd_dmc_goods;
