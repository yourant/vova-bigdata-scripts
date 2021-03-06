CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_ad_pause_history_arc
(
    `id` bigint COMMENT '',
    `ads_site_code` bigint COMMENT '',
    `channel` string COMMENT '',
    `ad_id` string COMMENT '',
    `status` string COMMENT '',
    `note` string COMMENT '',
    `date` string COMMENT ''
 )comment ''
PARTITIONED BY (pt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

INSERT overwrite table ods_fd_vb.ods_fd_ad_pause_history_arc PARTITION (pt='${hiveconf:pt}')
select  *
from tmp.tmp_fd_ad_pause_history_full;
