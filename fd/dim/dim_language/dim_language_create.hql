CREATE TABLE IF NOT EXISTS `dim.dim_fd_language`(
  `language_id` bigint COMMENT '语言id',
  `language_name` string COMMENT '语言名称',
  `language_code` string COMMENT '语言code')
COMMENT '语言维表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;