drop table dwb.dwb_vova_flashsale_cats;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_flashsale_cats
(
    cur_date             string COMMENT 'd_日期',
    datasource           string COMMENT 'd_datasource',
    platform             string COMMENT 'd_platform',
    first_cat_name       string COMMENT 'd_first_cat_name',
    all_goods_expre      bigint COMMENT 'i_all_goods_expre',
    brand_goods_expre    bigint COMMENT 'i_brand_goods_expre',
    no_brand_goods_expre bigint COMMENT 'i_no_brand_goods_expre',
    gmv                  double COMMENT 'i_gmv',
    brand_gmv            double COMMENT 'i_brand_gmv',
    no_brand_gmv         double COMMENT 'i_no_brand_gmv'
) COMMENT 'flashsale_cats' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
