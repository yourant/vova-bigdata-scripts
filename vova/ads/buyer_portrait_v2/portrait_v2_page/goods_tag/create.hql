-- 用户画像界面展示数据（由于数据量很大查询数仓，所以提前预处理好）
drop table if exists ads.ads_vova_goods_page_tag;
CREATE external TABLE `ads.ads_vova_goods_page_tag`(
  `goods_id` bigint COMMENT 'd_买家id',
  `first_cat_name` string COMMENT 'i_一级品类',
  `second_cat_name` string COMMENT 'i_二级品类',
  `brand_name` string COMMENT 'i_品牌',
  `shop_price` decimal(13,2) COMMENT 'i_售价',
  `gs_discount` string COMMENT 'i_折扣',
  `shipping_fee` string COMMENT 'i_运费',
  `mct_id` string COMMENT 'i_商家id',
  `gmv_1w` decimal(13,2) COMMENT 'i_近一周gmv',
  `sales_vol_1w` bigint COMMENT 'i_近一周销量',
  `clk_cnt_1w` bigint COMMENT 'i_近一周点击',
  `add_cat_cnt_1w` bigint COMMENT 'i_近一周加购数',
  `clk_rate_1w` decimal(13,2) COMMENT 'i_近一周点击率',
  `pay_rate_1w` decimal(13,2) COMMENT 'i_近一周支付转化率',
  `add_cat_rate_1w` decimal(13,2) COMMENT 'i_近一周加车转化率',
  `gmv_15d` decimal(13,2) COMMENT 'i_近15天gmv',
  `sales_vol_15d` bigint COMMENT 'i_近15天销量',
  `clk_cnt_15d` bigint COMMENT 'i_近15天点击',
  `add_cat_cnt_15d` bigint COMMENT 'i_近15天加购数',
  `clk_rate_15d` decimal(13,2) COMMENT 'i_近15天点击率',
  `pay_rate_15d` decimal(13,2) COMMENT 'i_近15天支付转化率',
  `add_cat_rate_15d` decimal(13,2) COMMENT 'i_近15天加车转化率',
  `gmv_1m` decimal(13,2) COMMENT 'i_近一个月gmv',
  `sales_vol_1m` bigint COMMENT 'i_近一个月销量',
  `clk_cnt_1m` bigint COMMENT 'i_近一个月点击',
  `add_cat_cnt_1m` bigint COMMENT 'i_近一个月加购数',
  `clk_rate_1m` decimal(13,2) COMMENT 'i_近一个月点击率',
  `pay_rate_1m` decimal(13,2) COMMENT 'i_近一个月支付转化率',
  `add_cat_rate_1m` decimal(13,2) COMMENT 'i_近一个月加车转化率',
  `goods_name` string COMMENT 'i_商品标题',
  `goods_desc` string COMMENT 'i_商品描述',
  `comment_cnt_6m` int COMMENT 'i_近180天评论数',
  `comment_good_cnt_6m` int COMMENT 'i_近180天好评数',
  `comment_bad_cnt_6m` int COMMENT 'i_近180天差评数')
  comment '页面展示商品标签表' PARTITIONED BY (gpt string)
     STORED AS PARQUETFILE;