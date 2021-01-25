drop table if exists dwb.dwb_vova_self_mct_top50;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_self_mct_top50
(
cur_date string COMMENT 'd_日期',
virtual_goods_id string COMMENT 'd_virtual_goods_id',
is_brand string COMMENT 'd_是否品牌',
expre_pv bigint COMMENT 'i_曝光pv',
clk_pv bigint COMMENT 'i_点击pv',
expre_uv bigint COMMENT 'i_曝光uv',
clk_uv bigint COMMENT 'i_点击uv',
gmv double COMMENT 'i_gmv',
cart_uv bigint COMMENT 'i_加购uv',
order_uv bigint COMMENT 'i_订单uv',
order_cnt bigint COMMENT 'i_下单量',
goods_num bigint COMMENT 'i_商品量',
pay_order_cnt bigint COMMENT 'i_订单数',
pay_uv bigint COMMENT 'i_支付uv'
) COMMENT '自营店铺top50'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


drop table if exists dwb.dwb_vova_self_mct_top50_his;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_self_mct_top50_his
(
cur_date string COMMENT 'd_日期',
virtual_goods_id string COMMENT 'd_virtual_goods_id',
is_brand string COMMENT 'd_是否品牌',
expre_pv bigint COMMENT 'i_曝光pv',
clk_pv bigint COMMENT 'i_点击pv',
expre_uv bigint COMMENT 'i_曝光uv',
clk_uv bigint COMMENT 'i_点击uv',
gmv double COMMENT 'i_gmv',
cart_uv bigint COMMENT 'i_加购uv',
order_uv bigint COMMENT 'i_订单uv',
order_cnt bigint COMMENT 'i_下单量',
goods_num bigint COMMENT 'i_商品量',
pay_order_cnt bigint COMMENT 'i_订单数',
pay_uv bigint COMMENT 'i_支付uv'
) COMMENT '自营店铺top50'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
