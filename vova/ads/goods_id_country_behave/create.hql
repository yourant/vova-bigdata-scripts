drop table ads.ads_vova_goods_id_country_behave;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_goods_id_country_behave
(
  goods_id bigint,
  country string,
  clicks bigint,
  impressions bigint,
  sales_order bigint,
  users bigint,
  impression_users bigint,
  gmv double,
  ctr double,
  gcr double,
  cr double,
  click_cr double
) COMMENT 'ads_goods_id_country_behave'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


