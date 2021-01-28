DROP TABLE dwb.dwb_vova_web_main_process;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_web_main_process
(
    event_date           string COMMENT 'd_date',
    datasource           string COMMENT 'd_datasource',
    region_code          string COMMENT 'd_region_code',
    platform             string COMMENT 'd_platform',
    original_channel     string COMMENT 'd_渠道来源',
    dau                  bigint COMMENT 'i_dau',
    home_page_uv         bigint COMMENT 'i_home_page_uv',
    cart_uv              bigint COMMENT 'i_cart_uv',
    checkout_uv          bigint COMMENT 'i_checkout_uv',
    product_detail_uv    bigint COMMENT 'i_product_detail_uv',
    add_cart_success_uv  bigint COMMENT 'i_add_cart_success_uv',
    continue_checkout_uv bigint COMMENT 'i_continue_checkout_uv',
    gmv                  DECIMAL(14, 2) COMMENT 'i_gmv',
    paid_uv              bigint COMMENT 'i_支付uv',
    paid_order_cnt       bigint COMMENT 'i_支付订单数',
    first_order_cnt      bigint COMMENT 'i_新用户支付uv'
) COMMENT 'dwb_vova_web_main_process' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

DROP TABLE dwb.dwb_vova_web_main_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_web_main_goods
(
    event_date           string COMMENT 'd_date',
    datasource           string COMMENT 'd_datasource',
    platform             string COMMENT 'd_platform',
    region_code          string COMMENT 'd_region_code',
    original_channel     string COMMENT 'd_渠道来源',
    impressions          bigint COMMENT 'i_impressions',
    impressions_uv       bigint COMMENT 'i_impressions_uv',
    clicks               bigint COMMENT 'i_clicks',
    clicks_uv            bigint COMMENT 'i_clicks_uv'
) COMMENT 'dwb_vova_web_main_goods' PARTITIONED BY (pt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

concat(round(impressions / impressions_uv * 100, 2), '%') as 人均曝光量,
concat(round(clicks / clicks_uv * 100, 2), '%') as 人均点击量,

concat(round(add_cart_success_uv / dau * 100, 2), '%') as 全局加购成功率,
concat(round(paid_uv / dau * 100, 2), '%') as 整体支付转化率,
round(order_cnt / paid_uv, 2) as 人均下单数,
round(gmv / paid_uv, 2) as 客单价,
round(gmv / paid_order_cnt, 2) as 笔单价,
concat(round(product_detail_uv / dau * 100, 2), '%') as 商品浏览率,
concat(round(add_cart_success_uv / product_detail_uv * 100, 2), '%') as 详情页加车率,
concat(round(checkout_uv / cart_uv * 100, 2), '%') as 加车页下单率,
concat(round(continue_checkout_uv / checkout_uv * 100, 2), '%') as Checkout页转化率,
concat(round(paid_uv / continue_checkout_uv * 100, 2), '%') as 支付成功率,
cast(if(add_cart_success_uv != 0, cast(COALESCE(dau,0.00)*100 as decimal(15,2) )/add_cart_success_uv  ,0.00) as varchar) || '%'
cast(if(dau != 0, cast(COALESCE(paid_uv,0.00)*100 as decimal(15,2) )/dau  ,0.00) as varchar) || '%'

