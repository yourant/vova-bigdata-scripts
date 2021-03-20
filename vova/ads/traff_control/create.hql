drop table ads.ads_vova_six_rank_mct;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_six_rank_mct(
  mct_id                bigint COMMENT '商家ID',
  mct_name              String COMMENT '商家',
  first_cat_id          bigint COMMENT '一级品类ID'
) COMMENT '商家等级表'
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
