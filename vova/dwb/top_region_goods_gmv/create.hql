drop table if exists dwb.dwb_region_top_gmv;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_region_top_gmv
(
    event_date    date COMMENT 'd_日期',
    region_code   string COMMENT 'd_国家',
    goods_id      bigint COMMENT 'd_goods_id',
    impression    bigint COMMENT 'i_impression',
    ctr           DECIMAL(15,4) COMMENT 'i_ctr',
    users          bigint COMMENT 'i_点击uv',
    rate          DECIMAL(15,4) COMMENT 'i_订单量/点击uv',
    orders         bigint COMMENT 'i_订单量',
    add_cart_rate DECIMAL(15,4) COMMENT 'i_加购成功率',
    gmv           DECIMAL(15,4) COMMENT 'i_gmv',
    gcr           DECIMAL(15,4) COMMENT 'i_gcr'
) COMMENT 'top100款商品查看报表'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;