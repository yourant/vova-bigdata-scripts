CREATE TABLE IF NOT EXISTS ods_fd_vb.ods_fd_ads_adgroup_daily_flat_report_arc (
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
	cost double,
	gmv double,
	`date` string,
	clicks int,
	impressions int,
	average_position double
) COMMENT 'erp 增量同步过来的ad report表'
PARTITIONED BY (dt STRING ) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

INSERT overwrite table ods_fd_vb.ods_fd_ads_adgroup_daily_flat_report_arc PARTITION (dt='${hiveconf:dt}')
select adfr_id, account_name, campaign_id, campaign_name, ad_group_id, ad_group_name, category, ads_site_code, country, ga_channel, channel, cost, gmv, `date`, clicks, impressions, average_position
from (
    select adfr_id, account_name, campaign_id, campaign_name, ad_group_id, ad_group_name, category, ads_site_code, country, ga_channel, channel, cost, gmv, `date`, clicks, impressions, average_position, 
        row_number () OVER (PARTITION BY adfr_id ORDER BY dt DESC) AS rank
    from(
        select 
            dt,
            adfr_id,
            account_name,
            campaign_id,
            campaign_name,
            ad_group_id,
            ad_group_name,
            category,
            ads_site_code,
            country,
            ga_channel,
            channel,
            cost,
            gmv,
            `date`,
            clicks,
            impressions,
            average_position
        from ods_fd_vb.ods_fd_ads_adgroup_daily_flat_report_arc where dt = '${hiveconf:dt_last}'

        UNION

        select 
            dt,
            adfr_id,
            account_name,
            campaign_id,
            campaign_name,
            ad_group_id,
            ad_group_name,
            category,
            ads_site_code,
            country,
            ga_channel,
            channel,
            cost,
            gmv,
            `date`,
            clicks,
            impressions,
            average_position
        from ods_fd_vb.ods_fd_ads_adgroup_daily_flat_report_inc where dt = '${hiveconf:dt}'
    )inc
) arc where arc.rank =1;
