CREATE TABLE IF NOT EXISTS  `ods_fd_vb.ods_fd_order_analytics`(
  `oa_id` bigint COMMENT '自增id',
  `order_id` bigint COMMENT '订单id',
  `source` string COMMENT '来源',
  `keyword` string COMMENT '关键词',
  `landing_page` string COMMENT '着陆页',
  `country` string COMMENT '国家',
  `region` string COMMENT '地区',
  `city` string COMMENT '城市',
  `browser` string COMMENT '浏览器',
  `screen_resolution` string COMMENT '屏幕尺寸',
  `campaign` string COMMENT '活动',
  `visitor_type` string COMMENT '访问类型',
  `operating_system` string COMMENT '操作系统',
  `adformat` string COMMENT '广告类型',
  `addisplayurl` string COMMENT '广告URL',
  `addestinationurl` string COMMENT '',
  `adwordscustomerid` string COMMENT '',
  `adgroup` string COMMENT '',
  `adwordscriteriaid` string COMMENT '',
  `addistributionnetwork` string COMMENT '',
  `admatchtype` string COMMENT '',
  `admatchedquery` string COMMENT '',
  `adwordscampaignid` string COMMENT '',
  `adwordsadgroupid` string COMMENT '',
  `adwordscreativeid` string COMMENT '',
  `devicecategory` string COMMENT '',
  `medium` string COMMENT '',
  `party_id` bigint COMMENT '',
  `order_sn` string COMMENT '订单编号',
  `medium_partition` string COMMENT '',
  `fullreferrer` string COMMENT '来源地址',
  `ad_content` string COMMENT '',
  `origin_source` string COMMENT '来源',
  `origin_medium` string COMMENT '投放类型',
  `ga_channel` string COMMENT '投放渠道',
  `last_update_time` string COMMENT '更新时间'
  )COMMENT 'artemis库同步的order_analytics表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_order_analytics
select `(dt)?+.+` from ods_fd_vb.ods_fd_order_analytics_arc where dt = '${hiveconf:dt}';
