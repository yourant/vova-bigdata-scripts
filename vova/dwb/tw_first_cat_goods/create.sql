DROP TABLE IF EXISTS dwb.dwb_vova_tw_first_cat_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_tw_first_cat_goods
(
cur_date string COMMENT 'd_日期',
platform string COMMENT 'd_平台',
main_channel string COMMENT 'd_渠道',
is_new string COMMENT 'd_是否新激活',
first_cat_name string COMMENT 'd_一级品类',
expre_uv string COMMENT 'i_曝光UV',
gmv string COMMENT 'i_GMV',
pay_sucess_order string COMMENT 'i_支付成功订单量',
avg_pay_cnt string COMMENT 'i_人均支付成功订单数',
pay_more_1_good string COMMENT 'i_出单商品数',
expre_gd string COMMENT 'i_有浏览的商品数',
avg_price string COMMENT 'i_客单价',
clk_rate string COMMENT 'i_点击率',
cge_rate string COMMENT 'i_转化率'
) COMMENT '台湾一级品类商品数据报表' PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' ;
