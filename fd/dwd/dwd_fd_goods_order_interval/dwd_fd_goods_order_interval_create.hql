create external table if not exists dwd.dwd_fd_goods_order_interval
(
    project           string    comment '组织',
    platform          string    comment '平台',
    country           string    comment '国家',
    language          string    comment '语言',
    cat_id            string    comment '类目ID',
    goods_id          bigint    comment '商品ID',
    order_num         bigint    comment '订单数量',
    paid_order_num    bigint    comment '下单成功数量'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;