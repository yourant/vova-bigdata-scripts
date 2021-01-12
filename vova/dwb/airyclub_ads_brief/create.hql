[4999](AC营销简报) | 广告数据交流简报
https://zt.gitvv.com/index.php?m=task&f=view&taskID=22572

任务描述
和营销技术开发刘进进（Jin Liu）对接，将报表展示在vova的superset上

需求描述
需要两块数据：
1. Airyclub Daily Ratio： 取自曲线图中airyclub的总gmv/cost/占比，以及主要渠道(fb,gg)的gmv/cost/占比
2. FB Ads CTR： 范围为：facebook account name包含“AC-”， 取自Facebook API数据
格式及数据要求请见附件
对接人：徐萌
  AiryClub Ads Brief.xlsx (12.94K)


mysql -h zkmarket.cznqgcwo1pjt.us-east-1.rds.amazonaws.com -u vomktread -pvova_mkt_read
report_ychen

1 ads_ga_channel_daily_gmv_flat_report
  ads_ga_channel_daily_flat_report
2 adwords_ad_performance_daily_report
  adwords_ad_carousel

mysql> desc ads_ga_channel_daily_gmv_flat_report;
+---------------+---------------+------+-----+---------+----------------+
| Field         | Type          | Null | Key | Default | Extra          |
+---------------+---------------+------+-----+---------+----------------+
| adgfr_id      | bigint(20)    | NO   | PRI | NULL    | auto_increment |
| ga_channel    | varchar(32)   | YES  | MUL | NULL    |                |
| country       | varchar(64)   | YES  | MUL | NULL    |                |
| ads_site_code | varchar(4)    | YES  | MUL | NULL    |                |
| category      | varchar(16)   | YES  | MUL | NULL    |                |
| channel       | varchar(64)   | YES  | MUL | NULL    |                |
| first_order   | tinyint(1)    | YES  | MUL | 0       |                |
| gmv           | decimal(20,2) | YES  |     | 0.00    |                |
| date          | date          | NO   | MUL | NULL    |                |
+---------------+---------------+------+-----+---------+----------------+
9 rows in set (0.00 sec)

mysql> desc ads_ga_channel_daily_flat_report;
+---------------+---------------+------+-----+---------+----------------+
| Field         | Type          | Null | Key | Default | Extra          |
+---------------+---------------+------+-----+---------+----------------+
| adfr_id       | bigint(20)    | NO   | PRI | NULL    | auto_increment |
| ga_channel    | varchar(32)   | YES  | MUL | NULL    |                |
| country       | varchar(64)   | YES  | MUL | NULL    |                |
| ads_site_code | varchar(2)    | YES  | MUL | NULL    |                |
| category      | varchar(16)   | YES  | MUL | NULL    |                |
| channel       | varchar(64)   | YES  | MUL | NULL    |                |
| cost          | decimal(20,2) | YES  |     | 0.00    |                |
| gmv           | decimal(20,2) | YES  |     | 0.00    |                |
| date          | date          | NO   | MUL | NULL    |                |
+---------------+---------------+------+-----+---------+----------------+
9 rows in set (0.00 sec)

1   用到表 ads_ga_channel_daily_gmv_flat_report
         ads_ga_channel_daily_flat_report


    字段ga_channel = facebook_api_android  为   fb-andr
             ga_channel = facebook_api_ios  为fb-ios
        ga_channel = google_api_android  为gg-andr
        ga_channel = google_api_ios   为gg-ios
Gmv   为两张表gmv和
cost  为   ads_ga_channel_daily_flat_report 中字段cost
ratio = gmv/cost


DROP TABLE rpt.rpt_airyclub_daily_ratio;
CREATE TABLE IF NOT EXISTS rpt.rpt_airyclub_daily_ratio
(
    total_gmv       DECIMAL(14, 2) COMMENT 'total_gmv',
    total_cost      DECIMAL(14, 2) COMMENT 'total_cost',
    total_ratio     DECIMAL(14, 2) COMMENT 'total_ratio',
    fb_ios_gmv      DECIMAL(14, 2) COMMENT 'fb_ios_gmv',
    fb_ios_cost     DECIMAL(14, 2) COMMENT 'fb_ios_cost',
    fb_ios_ratio    DECIMAL(14, 2) COMMENT 'fb_ios_ratio',
    fb_andr_gmv     DECIMAL(14, 2) COMMENT 'fb_andr_gmv',
    fb_andr_cost    DECIMAL(14, 2) COMMENT 'fb_andr_cost',
    fb_andr_ratio   DECIMAL(14, 2) COMMENT 'fb_andr_ratio',
    gc_ios_gmv      DECIMAL(14, 2) COMMENT 'gc_ios_gmv',
    gc_ios_cost     DECIMAL(14, 2) COMMENT 'gc_ios_cost',
    gc_ios_ratio    DECIMAL(14, 2) COMMENT 'gc_ios_ratio',
    gc_andr_gmv     DECIMAL(14, 2) COMMENT 'gc_andr_gmv',
    gc_andr_cost    DECIMAL(14, 2) COMMENT 'gc_andr_cost',
    gc_andr_ratio   DECIMAL(14, 2) COMMENT 'gc_andr_ratio'
) COMMENT 'Airyclub Daily Ratio'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


table2:
date campaign_contry campaign_device ad_name impression  clicks  ctr carousel_card   link_clicks


mysql> desc adwords_ad_performance_daily_report;
+--------------+------------------------+------+-----+---------+----------------+
| Field        | Type                   | Null | Key | Default | Extra          |
+--------------+------------------------+------+-----+---------+----------------+
| rec_id       | bigint(20) unsigned    | NO   | PRI | NULL    | auto_increment |
| account_id   | varchar(32)            | NO   |     | 0       |                |
| account_name | varchar(100)           | NO   | MUL |         |                |
| campaign_id  | varchar(32)            | NO   |     | 0       |                |
| adgroup_id   | varchar(32)            | NO   |     | 0       |                |
| ad_id        | varchar(32)            | NO   |     | 0       |                |
| ad_name      | varchar(100)           | NO   |     |         |                |
| clicks       | int(11) unsigned       | NO   |     | 0       |                |
| ctr          | decimal(20,4) unsigned | NO   |     | 0.0000  |                |
| link_clicks  | decimal(20,4)          | NO   |     | 0.0000  |                |
| start_date   | date                   | NO   | MUL | NULL    |                |
| end_date     | date                   | NO   |     | NULL    |                |
| impressions  | int(11) unsigned       | NO   |     | 0       |                |
+--------------+------------------------+------+-----+---------+----------------+
13 rows in set (0.00 sec)

mysql> desc adwords_ad_carousel;
+---------------+---------------------+------+-----+---------+----------------+
| Field         | Type                | Null | Key | Default | Extra          |
+---------------+---------------------+------+-----+---------+----------------+
| id            | bigint(20) unsigned | NO   | PRI | NULL    | auto_increment |
| account_name  | varchar(32)         | NO   | MUL |         |                |
| ad_id         | varchar(32)         | NO   |     |         |                |
| carousel_name | varchar(100)        | NO   |     |         |                |
| link_clicks   | int(11)             | NO   |     | 0       |                |
| start_date    | date                | YES  | MUL | NULL    |                |
| end_date      | date                | YES  |     | NULL    |                |
+---------------+---------------------+------+-----+---------+----------------+
7 rows in set (0.00 sec)

mysql> desc campaign_adgroup_mapping;
+------------------------+--------------+------+-----+---------+-------+
| Field                  | Type         | Null | Key | Default | Extra |
+------------------------+--------------+------+-----+---------+-------+
| AccountDescriptiveName | varchar(100) | NO   | PRI |         |       |
| CampaignId             | varchar(32)  | NO   | PRI |         |       |
| CampaignName           | varchar(128) | NO   |     |         |       |
| AdGroupId              | varchar(32)  | NO   | PRI |         |       |
| AdGroupName            | varchar(128) | NO   |     |         |       |
| adGroupCategory        | varchar(16)  | YES  |     | NULL    |       |
| adsSiteCode            | varchar(4)   | YES  | MUL | NULL    |       |
| campaignCountry        | varchar(32)  | YES  |     | NULL    |       |
| campaignChannel        | varchar(32)  | YES  | MUL | NULL    |       |
+------------------------+--------------+------+-----+---------+-------+
9 rows in set (0.00 sec)


用到表 adwords_ad_performance_daily_report
        adwords_ad_carousel
        campaign_adgroup_mapping
campaign

      Campaign contry  和device对应表 campaign_adgroup_mapping 中字段 campaignCountry 和 campaignChannel
     Ad name,impression,clicks,ctr为 adwords_ad_performance_daily_report 对应字段
      carousel_card,  link_clicks 为 adwords_ad_carousel 中的字段 carousel_name, link_clicks



DROP TABLE rpt.rpt_fb_ads_ctr;
CREATE TABLE IF NOT EXISTS rpt.rpt_fb_ads_ctr
(
    campaign_contry  string         COMMENT 'campaign_contry',
    campaign_device  string         COMMENT 'campaign_device',
    ad_name          string         COMMENT 'ad_name',
    carousel_card    string         COMMENT 'carousel_card',
    impressions      bigint         COMMENT 'impressions',
    clicks           bigint         COMMENT 'clicks',
    ctr              DECIMAL(14, 2) COMMENT 'ctr',
    link_clicks      bigint         COMMENT 'link_clicks'
) COMMENT 'FB Ads CTR'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
2021-01-09
ods_yx_cy.ods_yx_ads_ga_channel_daily_gmv_flat_report
ods_yx_cy.ods_yx_ads_ga_channel_daily_flat_report

ods_yx_cy.ods_yx_adwords_ad_performance_daily_report
ods_yx_cy.ods_yx_adwords_ad_carousel
ods_yx_cy.ods_yx_campaign_mapping

DROP TABLE dwb.dwb_vova_airyclub_daily_ratio;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_airyclub_daily_ratio
(
    total_gmv       DECIMAL(14, 4) COMMENT 'total_gmv',
    total_cost      DECIMAL(14, 4) COMMENT 'total_cost',
    total_ratio     DECIMAL(14, 4) COMMENT 'total_ratio',
    fb_ios_gmv      DECIMAL(14, 4) COMMENT 'fb_ios_gmv',
    fb_ios_cost     DECIMAL(14, 4) COMMENT 'fb_ios_cost',
    fb_ios_ratio    DECIMAL(14, 4) COMMENT 'fb_ios_ratio',
    fb_andr_gmv     DECIMAL(14, 4) COMMENT 'fb_andr_gmv',
    fb_andr_cost    DECIMAL(14, 4) COMMENT 'fb_andr_cost',
    fb_andr_ratio   DECIMAL(14, 4) COMMENT 'fb_andr_ratio',
    gc_ios_gmv      DECIMAL(14, 4) COMMENT 'gc_ios_gmv',
    gc_ios_cost     DECIMAL(14, 4) COMMENT 'gc_ios_cost',
    gc_ios_ratio    DECIMAL(14, 4) COMMENT 'gc_ios_ratio',
    gc_andr_gmv     DECIMAL(14, 4) COMMENT 'gc_andr_gmv',
    gc_andr_cost    DECIMAL(14, 4) COMMENT 'gc_andr_cost',
    gc_andr_ratio   DECIMAL(14, 4) COMMENT 'gc_andr_ratio'
) COMMENT 'Airyclub Daily Ratio'
PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_airyclub_daily_ratio/"
;

DROP TABLE dwb.dwb_vova_fb_ads_ctr;
CREATE external TABLE IF NOT EXISTS dwb.dwb_vova_fb_ads_ctr
(
    campaign_contry  string         COMMENT 'campaign_contry',
    campaign_device  string         COMMENT 'campaign_device',
    ad_name          string         COMMENT 'ad_name',
    carousel_card    string         COMMENT 'carousel_card',
    impressions      bigint         COMMENT 'impressions',
    clicks           bigint         COMMENT 'clicks',
    ctr              DECIMAL(14, 4) COMMENT 'ctr',
    link_clicks      bigint         COMMENT 'link_clicks'
) COMMENT 'FB Ads CTR'
PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://bigdata-offline/warehouse/dwb/dwb_vova_fb_ads_ctr/"
;
