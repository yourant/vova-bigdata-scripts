DROP TABLE ads.ads_vova_goods_performance;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_goods_performance
(
    goods_id          bigint,
    goods_sn          string COMMENT 'goods_sn',
    datasource        string,
    platform          string,
    region_code       string,
    impressions       bigint,
    clicks            bigint,
    users             bigint,
    sales_order       bigint,
    gmv               decimal(15, 2),
    ctr               decimal(15, 4),
    rate              decimal(15, 4),
    gr                decimal(15, 4),
    gcr               decimal(15, 4),
    last_update_time  TIMESTAMP,
    add_cart_cnt      bigint,
    first_cat_name    string,
    second_cat_name   string,
    first_cat_id      bigint,
    second_cat_id     bigint,
    shop_price_amount decimal(15, 2),
    is_on_sale        bigint,
    brand_id          bigint,
    brand_name        string,
    mct_name          string,
    mct_id            bigint,
    third_cat_id      string,
    third_cat_name    string,
    fourth_cat_id     string,
    fourth_cat_name   string
) COMMENT 'ads_vova_goods_performance' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

alter table ads.ads_vova_goods_performance ADD COLUMNS (
overall_score double COMMENT '综合评分'
) CASCADE;

DROP TABLE ads.ads_vova_site_goods_from_vova;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_site_goods_from_vova
(
    event_date      DATE COMMENT 'd_date',
    datasource      string COMMENT 'datasource',
    goods_id         bigint COMMENT '商品id',
    virtual_goods_id bigint COMMENT '商品虚拟id',
    impressions      bigint COMMENT '曝光数',
    gcr              decimal(15, 4) COMMENT 'gmv / users * clicks / impressions',
    goods_type      string COMMENT 'goods_type'
) COMMENT 'vova商品自动上架fd' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

#mysql
CREATE TABLE IF NOT EXISTS themis.ads_site_goods_from_vova
(
    id                int(11)          NOT NULL AUTO_INCREMENT,
    event_date        date             NOT NULL COMMENT 'd_日期',
    datasource        varchar(60)      NOT NULL DEFAULT '' COMMENT '站群',
    goods_id          int(11) UNSIGNED NOT NULL COMMENT '商品id',
    virtual_goods_id  int(11) UNSIGNED NOT NULL COMMENT '虚拟商品id',
    create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY (datasource, goods_id),
    KEY (goods_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8 COMMENT ='#vova商品自动上架站群';


#history
hadoop distcp -Dmapreduce.map.memory.mb=8096 -m 40 -overwrite  hdfs://ha-nn-uri/user/hive/warehouse/ads.db/ads_site_goods_from_vova  s3://bigdata-offline/warehouse/ads/ads_vova_site_goods_from_vova

MSCK REPAIR TABLE ads.ads_vova_site_goods_from_vova;

select event_date,datasource,count(*)
from ads.ads_vova_site_goods_from_vova
where pt >= '2021-02-20'
group by event_date,datasource
;

select event_date,datasource,count(*)
from themis.ads_site_goods_from_vova
group by event_date,datasource
