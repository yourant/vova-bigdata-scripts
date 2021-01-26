DROP TABLE dwb.dwb_vova_brand_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_brand_goods
(
    event_date        date COMMENT 'd_订单确认日期',
    datasource        string COMMENT 'd_datasource',
    region_code       string COMMENT 'd_国家',
    impressions       bigint COMMENT 'i_全站曝光量',
    brand_impressions bigint COMMENT 'i_brand商品曝光量',
    tot_gmv           DECIMAL(15, 2) COMMENT 'i_tot_gmv',
    brand_gmv         DECIMAL(15, 2) COMMENT 'i_brand_gmv'
) COMMENT 'brand商品占比'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


