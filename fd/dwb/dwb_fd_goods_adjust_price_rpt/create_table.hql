CREATE TABLE IF NOT EXISTS dwd.dwd_fd_order_goods_top (
`virtual_goods_id` string COMMENT '商品虚拟id',
`goods_id` string COMMENT '商品id',
`cat_name` string COMMENT '商品类别名',
`purchase_price` decimal(10,2) COMMENT '商品采购价',
`goods_type` string COMMENT '商品类型测款成功商品1/非测款成功商品0'
) COMMENT '当天商品销售量top的goods_id和virtual_goods_id'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

CREATE TABLE IF NOT EXISTS dwd.dwd_fd_goods_click_detail (
`goods_id` string COMMENT '虚拟商品id',
`virtual_goods_id` string COMMENT '虚拟商品id',
`project` string COMMENT '站点',
`country_code` string COMMENT '国家code',
`platform_type` string COMMENT '平台',
`add_session_id` string COMMENT 'add',
`goods_view_session_id` string COMMENT 'goods view',
`paid_order_id` string COMMENT '支付订单id',
`goods_click_session_id` string COMMENT 'click',
`goods_impression_session_id` string COMMENT 'impression',
`pt_date` string COMMENT '标明数据从原始表中的那个分区来的'
) COMMENT '商品点击事件信息'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

CREATE TABLE `dwb.dwb_fd_mid_goods_click_collect`(
  `virtual_goods_id` string COMMENT '虚拟商品id',
  `project` string COMMENT '站点',
  `country_code` string COMMENT '国家code',
  `platform_type` string COMMENT '平台',
  `add_rate_before` decimal(15,4) COMMENT '商品加车率-调价前',
  `add_rate_after` decimal(15,4) COMMENT '商品加车率-调价后',
  `rate_before` decimal(15,4) COMMENT '商品转化率-调价前',
  `rate_after` decimal(15,4) COMMENT '商品转化率-调价后',
  `ctr_before` decimal(15,4) COMMENT '商品点击率-调价前',
  `ctr_after` decimal(15,4) COMMENT '商品点击率-调价后',
  `cr_before` decimal(15,4) COMMENT '商品CR-调价前',
  `cr_after` decimal(15,4) COMMENT '商品CR-调价后',
  `click_before` bigint COMMENT '调价前点击会话数',
  `click_after` bigint COMMENT '调价后点击会话数',
  `impression_before` bigint COMMENT '调价前展示会话数',
  `impression_after` bigint COMMENT '调价后展示会话数')
COMMENT '商品点击事件信息'
PARTITIONED BY (pt STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");

CREATE TABLE `dwb.dwb_fd_goods_adjust_price_rpt`(
  `adjust_date` string COMMENT '调价时间',
  `goods_id` string COMMENT '商品id',
  `virtual_goods_id` string COMMENT '商品id',
  `country_code` string COMMENT '国家code',
  `project` string COMMENT '站点',
  `platform_type` string COMMENT '平台',
  `cat_name` string COMMENT '商品类别',
  `purchase_price` decimal(15,4) COMMENT '商品采购价',
  `avg_shop_price_before` decimal(15,4) COMMENT '平均销售价-调价前',
  `avg_shop_price_after` decimal(15,4) COMMENT '平均销售价-调价后',
  `adjust_price_range` decimal(15,4) COMMENT '调价幅度',
  `goods_number_before` bigint COMMENT '销售数量-调价前',
  `goods_number_after` bigint COMMENT '销售数量-调价后',
  `shop_price_before` decimal(15,4) COMMENT '销售额-调价前',
  `shop_price_after` decimal(15,4) COMMENT '销售额-调价后',
  `total_shop_price_before` decimal(15,4) COMMENT '总的销售额-调价前（全平台）',
  `total_shop_price_after` decimal(15,4) COMMENT '总的销售额-调价后（全平台）',
  `shop_amount_rate_before` decimal(15,4) COMMENT '销售金额占比-调价前',
  `shop_amount_rate_after` decimal(15,4) COMMENT '销售金额占比-调价后',
  `add_rate_before` decimal(15,4) COMMENT '加车率-调价前',
  `add_rate_after` decimal(15,4) COMMENT '加车率-调价后',
  `ctr_before` decimal(15,4) COMMENT '商品点击率CTR-调价前',
  `ctr_after` decimal(15,4) COMMENT '商品点击率CTR-调价后',
  `rate_before` decimal(15,4) COMMENT '商品转化率Rate-调价前',
  `rate_after` decimal(15,4) COMMENT '商品转化率Rate-调价后',
  `cr_before` decimal(15,4) COMMENT '商品CR-调价前',
  `cr_after` decimal(15,4) COMMENT '商品CR-调价后',
  `click_before` bigint COMMENT '调价前点击会话数',
  `click_after` bigint COMMENT '调价后点击会话数',
  `impression_before` bigint COMMENT '调价前展示会话数',
  `impression_after` bigint COMMENT '调价后展示会话数',
  `goods_type` string COMMENT '商品类型 测款成功商品 非测款成功商品')
COMMENT '商品调价前后：销售量，销售额，销售占比,CTR(商品点击率),Add_Rate(加车率),CR,Rate(商品转化率)'
PARTITIONED BY (pt STRING)
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY");



