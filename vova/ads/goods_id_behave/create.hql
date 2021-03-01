drop table ads.ads_goods_id_behave_m;
CREATE TABLE IF NOT EXISTS ads.ads_goods_id_behave_m
(
    goods_id                  bigint COMMENT '商品id',
    clicks                    bigint COMMENT '点击数',
    impressions               bigint COMMENT '曝光数',
    sales_order               bigint COMMENT '销量',
    users                     bigint COMMENT '点击uv',
    impression_users          bigint COMMENT '曝光uv',
    payed_user_num            bigint COMMENT '支付uv',
    gmv                       decimal(20, 8) COMMENT 'gmv',
    ctr                       decimal(20, 8) COMMENT 'ctr',
    gcr                       decimal(20, 8) COMMENT 'gcr',
    cr                        decimal(20, 8) COMMENT '支付转化率',
    click_cr                  decimal(20, 8) COMMENT 'click_cr',
    grr                       decimal(20, 8) COMMENT '非物流退款率',
    sor                       decimal(20, 8) COMMENT '7天上网率',
    lgrr                      decimal(20, 8) COMMENT '物流退款率',
    rate                      decimal(20, 8) COMMENT 'rate',
    gr                        decimal(20, 8) COMMENT 'gr',
    cart_rate                 decimal(20, 8) COMMENT 'cart_rate',
    cart_uv                   bigint COMMENT 'cart_uv',
    cart_pv                   bigint COMMENT 'cart_pv',
    shop_price                decimal(20, 8) COMMENT '商品价格',
    show_price                decimal(20, 8) COMMENT '商品价格加运费',
    brand_id                  bigint COMMENT '品牌id'
) COMMENT 'goods_id_behave_m'
PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;




drop table ads.ads_vova_goods_id_behave;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_id_behave
(
  goods_id bigint, 
  goods_sn string, 
  clicks bigint, 
  impressions bigint, 
  sales_order bigint, 
  users bigint, 
  impression_users bigint, 
  payed_user_num bigint, 
  gmv double,
  ctr double,
  gcr double,
  cr double,
  click_cr double,
  grr double,
  sor double,
  lgrr double,
  search_click bigint, 
  sales_order_m bigint, 
  gender bigint,
  comment double,
  search_score double,
  score double,
  rate double,
  gr double,
  cart_uv bigint, 
  cart_pv bigint, 
  cart_rate double,
  shop_price double,
  show_price double,
  brand_id bigint, 
  sld_gcr double
) COMMENT 'goods_id_behave'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

drop table ads.ads_vova_goods_sn_to_id;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_sn_to_id(
  goods_sn string,
  goods_id_list string,
  source string)
 COMMENT 'ads_goods_sn_to_id'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;