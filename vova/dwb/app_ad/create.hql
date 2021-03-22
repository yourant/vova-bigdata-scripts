DROP TABLE dwb.dwb_vova_ad_cost;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_ad_cost
(
    event_date      date COMMENT 'event_date',
    datasource      string COMMENT 'datasource',
    region_code     string COMMENT 'region_code',
    ga_channel      string COMMENT 'ga_channel',
    platform        string COMMENT 'platform',
    activate_dau    bigint COMMENT '当天激活用户数',
    tot_gmv         decimal(20, 2) COMMENT 'gmv',
    tot_cost        decimal(20, 2) COMMENT '当天广告花费',
    tot_region_gmv  decimal(20, 2) COMMENT '分国家不分渠道platform 的gmv',
    tot_region_cost decimal(20, 2) COMMENT '分国家不分渠道platform 的cost',
    gmv_7d          decimal(20, 2) COMMENT '210天前-180天前时间段30天每天近7天激活用户gmv',
    gmv_180d        decimal(20, 2) COMMENT '210天前-180天前时间段30天每天近180天激活用户gmv',
    est_gmv_7d      decimal(20, 2) COMMENT '当天广告花费'
) COMMENT 'dwb_vova_ad_cost' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_ad_gmv;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_ad_gmv
(
    datasource  string COMMENT 'datasource',
    region_code string COMMENT 'region_code',
    ga_channel  string COMMENT 'ga_channel',
    platform    string COMMENT 'platform',
    gmv_1d      decimal(20, 2) COMMENT 'gmv_1d',
    gmv_7d      decimal(20, 2) COMMENT 'gmv_7d',
    gmv_30d     decimal(20, 2) COMMENT 'gmv_30d',
    gmv_90d     decimal(20, 2) COMMENT 'gmv_90d',
    gmv_180d    decimal(20, 2) COMMENT 'gmv_180d'
) COMMENT 'dwb_vova_ad_gmv' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

--sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis_2.sh --db_code=yxl --table_name=temp_device_order_date_cohort --etl_type=ALL  --mapers=5 --period_type=day --partition_num=3 --split_id=install_date
--sh /mnt/vova-bigdata-scripts/common/sqoop_import_themis_2.sh --db_code=yxlc --table_name=temp_device_order_date_cohort --etl_type=ALL  --mapers=5 --period_type=day --partition_num=3 --split_id=install_date

insert overwrite table dwb.dwb_vova_ad_gmv PARTITION (pt)
select
/*+ REPARTITION(1) */
dd.datasource,
dd.region_code,
dd.ga_channel,
dd.platform,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 1,gmv,0)) AS gmv_1d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 7,gmv,0)) AS gmv_7d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 30,gmv,0)) AS gmv_30d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 90,gmv,0)) AS gmv_90d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 180,gmv,0)) AS gmv_180d,
trunc(dd.activate_date, 'MM') AS pt
from
tmp.tmp_dwb_vova_ad_gmv_base dd
where datediff(pay_date, activate_date) >= 0
AND datediff(pay_date, activate_date) <= 180
group by
dd.datasource,
dd.ga_channel,
dd.platform,
dd.region_code,
trunc(dd.activate_date, 'MM')

UNION ALL

select
/*+ REPARTITION(1) */
dd.datasource,
dd.region_code,
'all-ads' AS ga_channel,
dd.platform,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 1,gmv,0)) AS gmv_1d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 7,gmv,0)) AS gmv_7d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 30,gmv,0)) AS gmv_30d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 90,gmv,0)) AS gmv_90d,
sum(if(datediff(pay_date, activate_date) >= 0 AND datediff(pay_date, activate_date) <= 180,gmv,0)) AS gmv_180d,
trunc(dd.activate_date, 'MM') AS pt
from
tmp.tmp_dwb_vova_ad_gmv_base dd
inner join
(
select
distinct ga_channel
from
ods_yx_cy.ods_yx_ads_ga_channel_daily_flat_report
where cost> 0
) cost_ga_channel ON cost_ga_channel.ga_channel = dd.ga_channel
where datediff(pay_date, activate_date) >= 0
AND datediff(pay_date, activate_date) <= 180
group by
dd.datasource,
dd.platform,
dd.region_code,
trunc(dd.activate_date, 'MM')
;
