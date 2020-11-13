CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_campaign_adgroup_mapping_arc
(
    `AccountDescriptiveName` string COMMENT '',
    `CampaignId` string COMMENT '',
    `CampaignName` string COMMENT '',
    `AdGroupId` string COMMENT '',
    `AdGroupName` string COMMENT '',
    `adgroup_category` string COMMENT '',
    `ads_site_code` string COMMENT '',
    `campaign_country` string COMMENT '',
    `is_rem` bigint COMMENT '',
    `campaign_channel` string COMMENT ''
 )comment 'artemis库同步的'
PARTITIONED BY (dt STRING )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


INSERT overwrite table ods_fd_vb.ods_fd_campaign_adgroup_mapping_arc PARTITION (dt='${hiveconf:dt}')
select
    AccountDescriptiveName,
    CampaignId,
    CampaignName,
    AdGroupId,
    AdGroupName,
    adgroup_category,
    ads_site_code,
    campaign_country,
    is_rem,
    campaign_channel
from tmp.tmp_fd_campaign_adgroup_mapping_full;
