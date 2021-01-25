DROP TABLE IF EXISTS dwb.dwb_vova_homepage_information_collect;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_homepage_information_collect
(
cur_date string COMMENT 'd_日期',
country string COMMENT 'd_国家',
is_activate_user string COMMENT 'd_是否新激活',
element_type string COMMENT 'd_element_type',
is_brand string COMMENT 'd_是否brand',
first_cat_name string COMMENT 'd_一级品类',
second_cat_name string COMMENT 'd_二级品类',
entrance_expre string  COMMENT 'i_首页清单入口曝光数',
entrance_clk string COMMENT 'i_首页清单入口点击数',
entrance_clk_rate string COMMENT 'i_首页清单入口点击率',
list_detail_expre string COMMENT 'i_清单页面商品曝光数',
list_clk string COMMENT 'i_清单页面商品点击数',
list_clk_rate string COMMENT 'i_清单页面商品点击率',
list_detail_clk string COMMENT 'i_清单引导商详页浏览数',
list_cart_clk string COMMENT 'i_清单引导商详页加车数',
list_cart_rate string COMMENT 'i_清单引导加车率',
home_page_uv string COMMENT 'i_首页UV',
entrance_users string COMMENT 'i_首页清单入口曝光用户数',
list_uv string COMMENT 'i_清单页面UV',
list_detail_uv string COMMENT 'i_清单引导商详页UV',
list_cart_uv string COMMENT 'i_清单引导加车成功UV',
list_order_users string COMMENT 'i_清单引导下单用户数',
list_pay_users string COMMENT 'i_清单引导支付用户数',
list_pay_gmv string COMMENT 'i_清单引导成交GMV',
list_pay_gcr string COMMENT 'i_清单引导GCR',
list_expre string COMMENT 'i_首页清单入口曝光率',
list_user_change_rate string COMMENT 'i_首页清单入口用户转化率',
user_cart_rate string COMMENT 'i_用户加车率',
order_pay_rate string COMMENT 'i_下单-支付转化率',
detail_pay_rate string COMMENT 'i_商详-支付转化率'
) COMMENT '首页信息流汇总' PARTITIONED BY (pt STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;;


DROP TABLE IF EXISTS dwb.dwb_vova_homepage_information_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_homepage_information_goods
(
cur_date string COMMENT 'd_日期',
country string COMMENT 'd_国家',
is_activate_user string COMMENT 'd_是否新激活',
element_type string COMMENT 'd_element_type',
is_brand string COMMENT 'd_是否brand',
first_cat_name string COMMENT 'd_一级品类',
second_cat_name string COMMENT 'd_二级品类',
goods_id string  COMMENT 'i_goods_id',
virtual_goods_id string COMMENT 'i_virtual_goods_id',
shop_price string COMMENT 'i_shop_price',
shipping_fee string COMMENT 'i_shipping_fee',
price string COMMENT 'i_price',
goods_order_cnt string COMMENT 'i_goods_order_cnt',
detail_order_uv string COMMENT 'i_detail_order_uv',
pay_order_uv string COMMENT 'i_pay_order_uv',
detail_pay_uv string COMMENT 'i_detail_pay_uv',
detail_gmv string COMMENT 'i_detail_gmv',
gcr string COMMENT 'i_gcr',
avg_price string COMMENT 'i_avg_price',
goods_name string COMMENT 'i_goods_name'
) COMMENT '首页信息流商品' PARTITIONED BY (pt STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
