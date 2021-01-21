CREATE TABLE IF NOT EXISTS `dim.dim_fd_region`(
  `region_id` bigint COMMENT '地区id',
  `region_code` string COMMENT '地区代码',
  `region_name_en` string COMMENT '地区名英文',
  `region_name_cn` string COMMENT '地区名中文',
  `first_region_id` bigint COMMENT '一级地区id',
  `first_region_code` string COMMENT '一级地区代码',
  `first_region_name_en` string COMMENT '一级地区名英文',
  `first_region_name_cn` string COMMENT '一级地区名中文',
  `second_region_id` bigint COMMENT '二级地区id',
  `second_region_code` string COMMENT '二级地区代码',
  `second_region_name_en` string COMMENT '二级地区名英文',
  `second_region_name_cn` string COMMENT '二级地区名中文',
  `area_id` bigint COMMENT '区域ID',
  `area_name_en` string COMMENT '区域名称英文',
  `area_name_cn` string COMMENT '区域名称中文',
  `continent_id` bigint COMMENT '大洲id',
  `continent_name_en` string COMMENT '大洲名中文',
  `continent_name_cn` string COMMENT '大洲名英文'
)
COMMENT '地区信息维表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;