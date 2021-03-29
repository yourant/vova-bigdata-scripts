DROP TABLE ads.ads_vova_six_mct_flow_support_collector_data;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_six_mct_flow_support_collector_data
(
    goods_id         bigint,
    device_id        string,
    original_name    string,
    collector_ts     string,
    page_code        string
) COMMENT 'ads_vova_six_mct_flow_support_collector_data' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_six_mct_flow_support_goods_his;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_six_mct_flow_support_goods_his
(
    goods_id    bigint,
    impressions bigint,
    clicks      bigint,
    clicks_uv   bigint,
    gmv         decimal(20, 2),
    sales_order bigint,
    ctr         decimal(20, 4),
    gcr         decimal(20, 4)
) COMMENT 'ads_vova_six_mct_flow_support_goods_his' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

--mlb
DROP TABLE mlb.mlb_vova_six_mct_flow_support_d;
CREATE EXTERNAL TABLE IF NOT EXISTS mlb.mlb_vova_six_mct_flow_support_d
(
    goods_id             bigint,
    first_cat_id         bigint,
    brand_id             bigint,
    goods_name           string,
    img_vec              string,
    impressions          bigint COMMENT '商品累计曝光量',
    is_delete            bigint COMMENT '是否淘汰,1:delete;0:normal'
) COMMENT 'ads_vova_flow_support_d' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
LOCATION "s3://vova-mlb/REC/data/six_mct_rec/mlb_vova_six_mct_flow_support_d"
;

DROP TABLE ads.ads_vova_six_mct_flow_support_goods_page_process;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_six_mct_flow_support_goods_page_process
(
    goods_id    bigint,
    page_code   string,
    his_impressions bigint,
    t1_impressions  bigint,
    t2_impressions  bigint,
    t3_impressions  bigint,
    is_delete   bigint
) COMMENT 'ads_vova_six_mct_flow_support_goods_page_process' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE ads.ads_vova_six_mct_goods_flow_support_h;
CREATE EXTERNAL TABLE IF NOT EXISTS ads.ads_vova_six_mct_goods_flow_support_h
(
    goods_id    bigint,
    page_code   string
) COMMENT 'ads_vova_six_mct_goods_flow_support_h' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS themis.ads_vova_six_mct_goods_flow_support_h
(
    id                   int(11)          NOT NULL AUTO_INCREMENT,
    goods_id             int(11) UNSIGNED NOT NULL COMMENT '商品id',
    page_code            varchar(30)      NOT NULL DEFAULT '' COMMENT 'product_detail, product_list',
    create_time          timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_update_time     timestamp        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY goods_id (goods_id, page_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ads_vova_six_mct_goods_flow_support_h'
;
