drop table ads.ads_vova_goods_sn_behave;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_sn_behave
(
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
  rate double,
  gr double,
  cart_uv bigint,
  cart_pv bigint,
  cart_rate double,
  shop_price double,
  show_price double,
  brand_id bigint
) COMMENT 'ads_goods_sn_behave'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;