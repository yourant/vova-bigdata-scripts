CREATE TABLE IF NOT EXISTS ods_fd_ar.ods_fd_ads_adgroup_daily_flat_report (
    adfr_id bigint,
    account_name string,
    campaign_id string,
    campaign_name string,
    ad_group_id string,
    ad_group_name string,
    category string,
    ads_site_code string,
    country string,
    ga_channel string,
    channel string,
    cost decimal(15, 4),
    gmv decimal(15, 4),
    `date` string,
    clicks bigint,
    impressions bigint,
    average_position decimal(15, 4)
) COMMENT 'erp 增量同步过来的ad report表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ar.ods_fd_ads_adgroup_daily_flat_report
select `(dt)?+.+` from ods_fd_ar.ods_fd_ads_adgroup_daily_flat_report_arc where dt = '${hiveconf:dt}';
