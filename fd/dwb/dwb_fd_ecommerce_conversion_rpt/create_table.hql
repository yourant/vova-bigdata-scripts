CREATE TABLE  if not exists dwb.dwb_fd_ecommerce_conversion_rpt (
  `project_name` string ,
  `platform_type`  string,
  `country` string,
  `ga_channel` string,
  `add_uv` bigint ,
  `checkout_uv` bigint,
  `all_uv` bigint,
  `checkout_option_uv` bigint,
  `purchase_uv` bigint,
  `product_view_uv` bigint,
  `orders` bigint
)comment '打点数据session转化报表'
partitioned by(pt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");