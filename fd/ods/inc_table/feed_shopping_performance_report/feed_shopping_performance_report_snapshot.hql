CREATE TABLE IF NOT EXISTS ods_fd_ar.ods_fd_feed_shopping_performance_report (
    `fspr_id` bigint  COMMENT 'auto increase feed_shopping_performance_report id',
    `project` string COMMENT 'project name',
    `goods_id` bigint  COMMENT 'real goods id',
    `country` string COMMENT 'country code 2 digit',
    `language` string COMMENT 'language code 2 digit',
    `platform` string COMMENT 'campaign_name contains MB or not',
    `clicks` bigint ,
    `impressions` bigint ,
    `conversions` decimal(15, 4),
    `ctr` decimal(15, 4),
    `conversion_rate` decimal(15, 4),
    `insert_date` string 
) COMMENT ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ar.ods_fd_feed_shopping_performance_report
select `(dt)?+.+` from ods_fd_ar.ods_fd_feed_shopping_performance_report_arc where dt = '${hiveconf:dt}';
