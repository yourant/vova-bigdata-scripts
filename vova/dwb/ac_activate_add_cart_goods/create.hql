DROP TABLE dwb.dwb_ac_activate_add_cart_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_ac_activate_add_cart_goods
(
    event_date        date COMMENT 'd_订单确认日期',
    virtual_goods_id  bigint COMMENT 'i_virtual_goods_id',
    goods_id          bigint COMMENT 'i_goods_id',
    second_cat_name   string COMMENT 'd_second_cat_name',
    mct_name          string COMMENT 'i_店铺名',
    region_code       string COMMENT 'd_国家',
    shop_price_amount DECIMAL(15, 2) COMMENT 'i_商品价格',
    add_cart_uv       bigint COMMENT 'i_加购uv',
    sale_cnt          bigint COMMENT 'i_销量',
    paid_order_cnt    bigint COMMENT 'i_支付订单数',
    paid_uv           bigint COMMENT 'i_支付uv',
    gmv               DECIMAL(15, 2) COMMENT 'i_支付uv'
) COMMENT 'ac-新激活用户加购数据'
    PARTITIONED BY ( pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;


