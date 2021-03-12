DROP TABLE dwb.dwb_vova_conversion_monitor;
CREATE EXTERNAL TABLE IF NOT EXISTS dwb.dwb_vova_conversion_monitor
(
    goods_id               bigint COMMENT 'goods_id',
    order_cnt              bigint COMMENT 'order_cnt',
    buyer_cnt              bigint COMMENT 'buyer_cnt',
    web_order_cnt          bigint COMMENT 'web_order_cnt',
    expre                  bigint COMMENT 'expre',
    new_user_cnt           bigint COMMENT 'new_user_cnt',
    new_user_order_cnt     bigint COMMENT 'new_user_order_cnt',
    sec_order_cnt          bigint COMMENT 'sec_order_cnt',
    sec_buyer_cnt          bigint COMMENT 'sec_buyer_cnt',
    sec_web_order_cnt      bigint COMMENT 'sec_web_order_cnt',
    sec_expre              bigint COMMENT 'sec_expre',
    sec_new_user_cnt       bigint COMMENT 'sec_new_user_cnt',
    sec_new_user_order_cnt bigint COMMENT 'sec_new_user_order_cnt',
    web_order_rate         DECIMAL(15, 6) COMMENT '网站下单率:昨天非app渠道的子订单数/昨天总子订单数',
    web_order_rate_ratio   DECIMAL(15, 6) COMMENT '网站下单率之比:此商品的网站下单率/此商品类目的网站下单率',
    expre_efficiency       DECIMAL(15, 6) COMMENT '曝光效率:昨天商品总曝光/昨天总子订单数',
    expre_efficiency_ratio DECIMAL(15, 6) COMMENT '曝光效率之比:此商品的曝光效率/此商品类目的曝光效率',
    new_user_rate          DECIMAL(15, 6) COMMENT '商品新客率:昨天新用户数/昨天总支付用户数',
    new_user_rate_ratio    DECIMAL(15, 6) COMMENT '商品新客率之比:此商品的商品新客率/此商品类目的商品新客率',
    new_user_order_rate    DECIMAL(15, 6) COMMENT '新用户订单占比:昨天新用户子订单数/昨天总子订单数',
    new_user_order_ratio     DECIMAL(15, 6) COMMENT '新用户订单占比之比:此商品的新用户订单占比/此商品类目的新用户订单占比'
) COMMENT 'dwb_vova_conversion_monitor' PARTITIONED BY (pt STRING)
    STORED AS PARQUETFILE;

DROP TABLE tmp.tmp_dwb_vova_conversion_monitor_sec_cat;
CREATE EXTERNAL TABLE IF NOT EXISTS tmp.tmp_dwb_vova_conversion_monitor_sec_cat
(
    second_cat_id        bigint COMMENT 'second_goods_id',
    order_cnt              bigint COMMENT 'order_cnt',
    buyer_cnt              bigint COMMENT 'buyer_cnt',
    web_order_cnt          bigint COMMENT 'web_order_cnt',
    expre                  bigint COMMENT 'expre',
    new_user_cnt           bigint COMMENT 'new_user_cnt',
    new_user_order_cnt     bigint COMMENT 'new_user_order_cnt',
    web_order_rate         DECIMAL(15, 6) COMMENT '网站下单率:昨天非app渠道的子订单数/昨天总子订单数',
    expre_efficiency       DECIMAL(15, 6) COMMENT '曝光效率:昨天商品总曝光/昨天总子订单数',
    new_user_rate          DECIMAL(15, 6) COMMENT '商品新客率:昨天新用户数/昨天总支付用户数',
    new_user_order_rate    DECIMAL(15, 6) COMMENT '新用户订单占比:昨天新用户子订单数/昨天总子订单数'
) COMMENT 'tmp_dwb_vova_conversion_monitor_sec_cat'
    STORED AS PARQUETFILE;

