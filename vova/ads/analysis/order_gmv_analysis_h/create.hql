CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_order_gmv_analysis_h
(
    country   string        comment '国家代码',
    order_cnt bigint        comment '下单量',
    gmv       decimal(13,2) comment 'gmv'
) COMMENT '本日分国家订单gmv数据' PARTITIONED BY (pt String, hour String) STORED AS PARQUETFILE;


alter table ads.ads_vova_order_gmv_analysis_h add columns(order_cnt_growth_rate decimal(13,2) comment '订单量增长率') cascade;
alter table ads.ads_vova_order_gmv_analysis_h add columns(gmv_growth_rate       decimal(13,2) comment 'gmv增长率');
alter table ads.ads_vova_order_gmv_analysis_h add columns(country_name_cn       string comment '城市中文名称') cascade;
