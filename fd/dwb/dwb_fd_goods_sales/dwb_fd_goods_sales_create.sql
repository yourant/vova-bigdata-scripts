
CREATE TABLE dwb.dwb_fd_goods_sales (
  `project_name` string COMMENT '组织',
  `cat_name` string COMMENT '品类',
  `country_code` string COMMENT '国家',
  `source_type` string COMMENT '平台',
  `virtual_goods_id` string COMMENT '商品虚拟ID',
  `goods_amount_daycount` decimal(15,4)  COMMENT '当日销售额',
  `goods_number_daycount` bigint COMMENT '当日销量'
  )comment '商品销售统计'
 partitioned by (pt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as PARQUET;
