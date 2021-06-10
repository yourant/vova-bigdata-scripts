-- 分析平台商家分析查询表
CREATE external TABLE ads.ads_vova_merchant_gmv_analysis_h(
  `mct_id`           int            COMMENT 'd_店铺id',
  `mct_name`         string         COMMENT 'd_店铺名称',
  `mct_rank`         int            COMMENT 'd_店铺等级',
  `is_brand`         int            COMMENT 'd_是否品牌',
  `gmv`              decimal(13,2)  COMMENT 'i_gmv'
)COMMENT '分析平台商家gmv分析小时表' PARTITIONED BY (pt STRING, hour String)   stored as parquetfile;