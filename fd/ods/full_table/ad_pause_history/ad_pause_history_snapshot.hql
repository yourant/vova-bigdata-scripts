CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_ad_pause_history
(
    `id` bigint COMMENT '',
    `ads_site_code` bigint COMMENT '',
    `channel` string COMMENT '',
    `ad_id` string COMMENT '',
    `status` string COMMENT '',
    `note` string COMMENT '',
    `date` string COMMENT ''
 )comment ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_ad_pause_history
select `(dt)?+.+` from ods_fd_vb.ods_fd_ad_pause_history_arc 
where dt = '${hiveconf:dt}';
