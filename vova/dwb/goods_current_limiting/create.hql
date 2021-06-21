drop table dwb.dwb_vova_goods_current_limiting;
CREATE EXTERNAL TABLE dwb.dwb_vova_goods_current_limiting
(
    goods_id               bigint COMMENT '商品id',
    is_alone            boolean COMMENT '是否为孤品',
    group_id          bigint        COMMENT '商品组id',
    shop_price             decimal(13,2) COMMENT '商品价格',
    is_cheapest              boolean COMMENT '是否为组内最低价'
) COMMENT '商品限流邮件数据'
    PARTITIONED BY (pt string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE
;