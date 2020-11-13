CREATE TABLE IF NOT EXISTS tmp.tmp_fd_dmc_competing_website_tort (
    value STRING
) 
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/dmc_competing_website_tort';

msck repair table tmp.tmp_fd_dmc_competing_website_tort;
