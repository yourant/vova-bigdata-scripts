CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_fd_dmc_party (
    value STRING
) 
PARTITIONED BY (pt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE
LOCATION '${hiveconf:flume_path}/dmc_party';

msck repair table tmp.tmp_fd_dmc_party;
