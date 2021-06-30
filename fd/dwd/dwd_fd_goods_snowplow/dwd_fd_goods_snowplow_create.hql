create external table if not exists dwd.dwd_fd_goods_snowplow
(
    event_time        string    comment '事件时间',
    record_type       string    comment '类型',
    project           string    comment '组织',
    user_id           string    comment '用户ID',
    domain_userid     string    comment '用户domainID',
    session_id        string    comment 'session ID',
    platform_type     string    comment '平台',
    country           string    comment '国家',
    language          string    comment '语言',
    cat_id            string    comment '类目ID',
    goods_id          bigint    comment '商品ID',
    virtual_goods_id  string    comment '虚拟商品ID',
    page_code         string    comment '页面标识',
    list_type         string    comment '类型列表',
    absolute_position bigint    comment '绝对位置',
    url_route_sn      string    comment 'URL路由',
    event_num         bigint    comment '事件数量',
    order_id          bigint    comment '订单ID',
    order_num         bigint    comment '订单数量',
    paying_order_num  bigint    comment '正在付款数量',
    paid_order_num    bigint    comment '下单成功数量'
) PARTITIONED BY (
    `pt` string,
    `hour` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;