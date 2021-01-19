DROP TABLE dim.dim_zq_order_goods;
CREATE EXTERNAL TABLE IF NOT EXISTS dim.dim_zq_order_goods
(
    datasource       STRING COMMENT 'datasource',
    region_code      STRING COMMENT 'region_code',
    platform         STRING COMMENT 'platform',
    from_domain      STRING COMMENT 'oi.from_domain',
    order_id         BIGINT COMMENT 'oi.order_id',
    pay_time         timestamp COMMENT 'oi.pay_time',
    order_time       timestamp COMMENT 'oi.order_time',
    order_goods_id   BIGINT COMMENT 'og.rec_id',
    goods_id         BIGINT COMMENT 'og.goods_id',
    virtual_goods_id BIGINT COMMENT 'vg.virtual_goods_id',
    buyer_id         BIGINT COMMENT 'oi.user_id',
    goods_number     BIGINT COMMENT 'og.goods_number',
    shop_price       decimal(16, 4) COMMENT 'og.shop_price',
    gmv              decimal(16, 4) COMMENT 'gmv',
    domain_userid    STRING COMMENT 'domain_userid'
) COMMENT '子订单维度'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;