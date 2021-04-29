drop table ads.ads_vova_six_rank_mct;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_six_rank_mct(
  mct_id                bigint COMMENT '商家ID',
  mct_name              String COMMENT '商家',
  first_cat_id          bigint COMMENT '一级品类ID'
) COMMENT '商家等级表'
 PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table ads.ads_vova_six_rank_mct_arc;
CREATE external TABLE IF NOT EXISTS ads.ads_vova_six_rank_mct_arc(
  mct_id                bigint COMMENT '商家ID',
  mct_name              String COMMENT '商家',
  first_cat_id          bigint COMMENT '一级品类ID',
  is_delete             bigint COMMENT '是否有效'
) COMMENT '六级商家等级表'
  PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;