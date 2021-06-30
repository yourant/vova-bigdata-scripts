create external table if not exists tmp.tmp_fd_goods_uv
(
    goods_id          string    comment '商品ID',
    cat_id            string    comment '分类ID',
    record_type       string    comment '类型',
    project           string    comment '组织',
    country           string    comment '国家',
    language          string    comment '语言',
    platform          string    comment '平台类型',
    page_code         string    comment '页面标识',
    list_type         string    comment '类型列表',
    goods_uv          bigint    comment '商品日UV',
    event_num         bigint    comment '事件条数',
    order_num         bigint    comment '订单数量',
    paying_order_num  bigint    comment '正在付款数量',
    paid_order_num    bigint    comment '已经付款数量'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
