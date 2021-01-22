DROP TABLE IF EXISTS dwb.dwb_vova_app_goods_guv;
CREATE TABLE IF NOT EXISTS dwb.dwb_vova_app_goods_guv
(
    event_date           date  COMMENT 'd_日期',
    region_code          string COMMENT 'd_region_code',
    platform             string COMMENT 'd_platform',
    first_cat_name       string COMMENT 'd_first_cat_name',
    second_cat_name      string COMMENT 'd_second_cat_name',
    third_cat_name       string COMMENT 'd_third_cat_name',
    is_brand             string COMMENT 'd_is_brand',
    impressions_guv      bigint COMMENT 'i_impressions_guv',
    impressions_uv       bigint COMMENT 'i_impressions_uv',
    impressions          bigint COMMENT 'i_impressions',
    click_guv            bigint COMMENT 'i_click_guv',
    pd2cart_guv          bigint COMMENT 'i_pd2cart_guv',
    pd2cart_success_guv  bigint COMMENT 'i_pd2cart_success_guv',
    pd2cart_referrer_guv bigint COMMENT 'i_pd2cart_referrer_guv',
    paid_guv             bigint COMMENT 'i_paid_guv',
    order_guv            bigint COMMENT 'i_order_guv'
) COMMENT 'dwb_vova_app_goods_guv'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


