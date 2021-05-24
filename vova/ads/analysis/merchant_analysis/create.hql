-- 分析平台商家分析查询表
CREATE external TABLE ads.ads_vova_merchant_analysis(
  `datasource`       string         COMMENT 'i_datasource',
  `device_id`        string         COMMENT 'i_设备id',
  `goods_id`         bigint         COMMENT 'i_商品id',
  `page_code`        string         COMMENT 'i_页面code',
  `list_type`        string         COMMENT 'i_list_type',
  `clk_cnt`          bigint         COMMENT 'd_点击次数',
  `expre_cnt`        bigint         COMMENT 'd_曝光数',
  `sales_vol`        bigint         COMMENT 'd_销量',
  `gmv`              decimal(13,2)  COMMENT 'd_gmv',
  `is_brand`         string         COMMENT 'd_是否品牌，all/Y/N',
  `first_cat_name`   string         COMMENT 'd_一级品类名称',
  `first_cat_id`     bigint         COMMENT 'd_一级品类id',
  `second_cat_name`  string         COMMENT 'd_二级品类名称',
  `second_cat_id`    bigint         COMMENT 'd_二级品类id',
  `mct_id`           int            COMMENT 'd_店铺id',
  `mct_name`         string         COMMENT 'd_店铺名称',
  `mct_rank`         int            COMMENT 'd_店铺等级'
)COMMENT '分析平台商家分析查询表' PARTITIONED BY (pt STRING)   stored as parquetfile;