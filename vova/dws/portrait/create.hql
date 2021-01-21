drop table if exists dws.dws_vova_buyer_portrait;
CREATE EXTERNAL TABLE if not exists dws.dws_vova_buyer_portrait
(
    `datasource`                STRING COMMENT '数据平台',
    `buyer_id`                  BIGINT COMMENT '买家id',
    `gender`                    STRING COMMENT '性别',
    `age_range`                 STRING COMMENT '年龄段',
    `country`                   STRING COMMENT '国家',
    `language`                  STRING COMMENT '语言',
    `platform`                  STRING COMMENT '设备平台(android, ios, pc, mob, unknown)',
    `email`                     STRING COMMENT '邮箱',
    `reg_method`                STRING COMMENT '注册方式',
    `reg_time`                  TIMESTAMP COMMENT '注册时间',
    `first_cat_prefer_1w`       ARRAY<BIGINT> COMMENT '近7天一级品类偏好top10',
    `first_cat_prefer_1m`       ARRAY<BIGINT> COMMENT '近30天一级品类偏好top10',
    `first_cat_prefer_his`      ARRAY<BIGINT> COMMENT '近一年一级品类偏好top10',
    `second_cat_prefer_1w`      ARRAY<BIGINT> COMMENT '近7天二级品类偏好top10,如果不存在二级品类则取一级',
    `second_cat_prefer_1m`      ARRAY<BIGINT> COMMENT '近30天二级品类偏好top10,如果不存在二级品类则取一级',
    `second_cat_prefer_his`     ARRAY<BIGINT> COMMENT '近一年二级品类偏好top10,如果不存在二级品类则取一级',
    `second_cat_max_click_1m`   BIGINT COMMENT '近一个月点击最多二级品类，如果不存在二级品类则取一级',
    `second_cat_max_collect_1m` BIGINT COMMENT '近一个月收藏最多二级品类，如果不存在二级品类则取一级',
    `second_cat_max_cart_1m`    BIGINT COMMENT '近一个月加购最多二级品类，如果不存在二级品类则取一级',
    `second_cat_max_order_1m`   BIGINT COMMENT '近一个月下单最多二级品类，如果不存在二级品类则取一级',
    `brand_prefer_1w`           ARRAY<BIGINT> COMMENT '近7天品牌偏好top10',
    `brand_prefer_1m`           ARRAY<BIGINT> COMMENT '近30天品牌偏好top10',
    `brand_prefer_his`          ARRAY<BIGINT> COMMENT '历史品牌偏好top10',
    `brand_max_click_1m`        BIGINT COMMENT '近30天点击最多品牌',
    `brand_max_collect_1m`      BIGINT COMMENT '近30天收藏最多品牌',
    `brand_max_cart_1m`         BIGINT COMMENT '近30天加购最多品牌',
    `brand_max_order_1m`        BIGINT COMMENT '近30天下单最多品牌',
    `active_day_1m`             STRING COMMENT '近30天每日活跃时段',
    `active_week_his`           STRING COMMENT '历史每周活跃时段',
    `active_month_his`          STRING COMMENT '历史每月活跃时段',
    `price_prefer_1w`           STRING COMMENT '近7天价格偏好层级',
    `pay_cnt_his`               BIGINT COMMENT '历史支付次数',
    `ship_cnt_his`              BIGINT COMMENT '历史发货次数',
    `max_visits_cnt_cw`         BIGINT COMMENT '历史最大访问次数'
) PARTITIONED BY ( pt string)
 COMMENT '买家画像结果表'
    STORED AS PARQUETFILE;

DROP TABLE IF EXISTS tmp.tmp_vova_dws_buyer_portrait_cat7_result;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_dws_buyer_portrait_cat7_result
(
    datasource          STRING,
    buyer_id            BIGINT,
    first_cat_prefer_1w ARRAY<BIGINT>
) COMMENT 'tmp_vova_dws_buyer_portrait_cat7_result'
    STORED AS PARQUETFILE;

DROP TABLE IF EXISTS tmp.tmp_vova_dws_buyer_portrait_cat30_result;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_dws_buyer_portrait_cat30_result
(
    datasource          STRING,
    buyer_id            BIGINT,
    first_cat_prefer_1m ARRAY<BIGINT>
) COMMENT 'tmp_vova_dws_buyer_portrait_cat30_result'
    STORED AS PARQUETFILE;

DROP TABLE IF EXISTS tmp.tmp_vova_dws_buyer_portrait_first_cat_his_result;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_dws_buyer_portrait_first_cat_his_result
(
    datasource          STRING,
    buyer_id            BIGINT,
    first_cat_prefer_his ARRAY<BIGINT>
) COMMENT 'tmp_vova_dws_buyer_portrait_first_cat_his_result'
    STORED AS PARQUETFILE;

DROP TABLE IF EXISTS tmp.tmp_vova_dws_buyer_portrait_second_cat7_result;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_dws_buyer_portrait_second_cat7_result
(
    datasource          STRING,
    buyer_id            BIGINT,
    second_cat_prefer_1w ARRAY<BIGINT>
) COMMENT 'tmp_vova_dws_buyer_portrait_second_cat7_result'
    STORED AS PARQUETFILE;

DROP TABLE IF EXISTS tmp.tmp_vova_dws_buyer_portrait_second_cat30_result;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_dws_buyer_portrait_second_cat30_result
(
    datasource          STRING,
    buyer_id            BIGINT,
    second_cat_prefer_1m ARRAY<BIGINT>
) COMMENT 'tmp_vova_dws_buyer_portrait_second_cat30_result'
    STORED AS PARQUETFILE;

DROP TABLE IF EXISTS tmp.tmp_vova_dws_buyer_portrait_second_cat_his_result;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_dws_buyer_portrait_second_cat_his_result
(
    datasource          STRING,
    buyer_id            BIGINT,
    second_cat_prefer_his ARRAY<BIGINT>
) COMMENT 'tmp_vova_dws_buyer_portrait_second_cat_his_result'
    STORED AS PARQUETFILE;

DROP TABLE IF EXISTS tmp.tmp_vova_dws_buyer_portrait_brand7_result;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_dws_buyer_portrait_brand7_result
(
    datasource          STRING,
    buyer_id            BIGINT,
    brand_prefer_1w ARRAY<BIGINT>
) COMMENT 'tmp_vova_dws_buyer_portrait_brand7_result'
    STORED AS PARQUETFILE;

DROP TABLE IF EXISTS tmp.tmp_vova_dws_buyer_portrait_brand30_result;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_dws_buyer_portrait_brand30_result
(
    datasource          STRING,
    buyer_id            BIGINT,
    brand_prefer_1m ARRAY<BIGINT>
) COMMENT 'tmp_vova_dws_buyer_portrait_brand30_result'
    STORED AS PARQUETFILE;

DROP TABLE IF EXISTS tmp.tmp_vova_dws_buyer_portrait_brand_his_result;
CREATE TABLE IF NOT EXISTS tmp.tmp_vova_dws_buyer_portrait_brand_his_result
(
    datasource          STRING,
    buyer_id            BIGINT,
    brand_prefer_his ARRAY<BIGINT>
) COMMENT 'tmp_vova_dws_buyer_portrait_brand_his_result'
    STORED AS PARQUETFILE;












