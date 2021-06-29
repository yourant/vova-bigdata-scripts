create external table if not exists dwb.dwb_fd_goods_snowplow_uv
(
    record_type       string    comment '类型',
    project           string    comment '组织',
    platform_type     string    comment '平台类型',
    country           string    comment '国家',
    language          string    comment '语言',
    cat_id            string    comment '分类ID',
    goods_id          string    comment '商品ID',
    page_code         string    comment '页面标识',
    list_type         string    comment '类型列表',
    goods_uv          bigint    comment '商品日UV',
    event_num         bigint    comment '事件条数',
    order_num         bigint    comment '订单数量',
    paying_order_num  bigint    comment '正在付款数量',
    paid_order_num    bigint    comment '已经付款数量'
) PARTITIONED BY (
    `pt` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;