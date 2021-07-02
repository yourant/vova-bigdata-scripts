create external table if not exists ads.ads_fd_goods_display_order_artemis_language_interval
(
    goods_id                bigint          comment '商品ID',
    language_id             bigint          comment '语言ID',
    project_name            string          comment '组织',
    platform                string          comment '平台',
    impressions             string          comment '预售商品品类列表曝光UV',
    clicks                  bigint          comment '预售商品品类列表点击UV',
    users                   bigint          comment '商品详情页UV',
    sales_order             bigint          comment '销量排序',
    detail_add_cart         bigint          comment '详情页加车UV',
    list_add_cart           bigint          comment '品类列表加车UV',
    checkout                bigint          comment '支付订单数',
    virtual_sales_order     bigint          comment '默认0',
    goods_order             bigint          comment '默认0',
    start_time              timestamp       comment '开始时间',
    end_time                timestamp       comment '结束时间',
    `interval`              string          comment '标记',
    is_active               bigint          comment '默认1',
    sales                   bigint          comment '商品销量（即销售件数）'
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;
