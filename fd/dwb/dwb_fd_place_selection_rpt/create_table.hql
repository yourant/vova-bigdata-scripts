CREATE TABLE if not exists dwb.dwb_fd_place_selection_rpt (
  `data_type` string COMMENT '数据类型',
  `project_name` string COMMENT '网站组织名',

  `goods_id` bigint  COMMENT '商品真实id',
  `virtual_goods_id` bigint COMMENT '商品虚拟id',

  `country_code` string COMMENT '国家',
  `cat_name` string COMMENT '品类名称',
  `platform` string COMMENT '网站平台(web/h5/mob)',
  `impressions` bigint COMMENT '页面展示数',
  `sales_order` bigint COMMENT '商品销售件数',
  `clicks` bigint COMMENT '页面点击数',
  `users` bigint COMMENT '访问用户数',
  `ctr` DECIMAL(15, 4) COMMENT 'ctr',
  `cr` DECIMAL(15, 4) COMMENT 'cr',
  `country_cat_platform_top` bigint COMMENT '国家+品类+设备粒度cr排名',
  `cat_platform_top` bigint COMMENT '品类+设备粒度cr排名'

) COMMENT'投放选款报表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");