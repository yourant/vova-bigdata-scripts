CREATE TABLE IF NOT EXISTS tmp.tmp_fd_romeo_currency_conversion(
    value STRING
) 
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/currency_conversion';

MSCK REPAIR TABLE tmp.tmp_fd_romeo_currency_conversion;
