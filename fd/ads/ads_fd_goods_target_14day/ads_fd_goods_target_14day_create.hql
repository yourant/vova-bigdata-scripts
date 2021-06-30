create external table if not exists ads.ads_fd_goods_target_14day
(
    goods_id             bigint        comment '商品ID',
    cat_id               string        comment '类目ID',
    country              string        comment '国家',
    project              string        comment '组织',
    platform_type        string        comment '平台',
    impressions          bigint        comment '预售商品品类列表曝光UV',
    click                bigint        comment '预售商品品类列表点击UV',
    users                bigint        comment '商品详情页UV',
    add_session          bigint        comment '加车UV',
    product_add_session  bigint        comment '详情页加车UV',
    orders               bigint        comment '支付订单数'
) comment '商品14天指标表现表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
