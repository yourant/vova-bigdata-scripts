CREATE TABLE IF NOT EXISTS `dwd.dwd_fd_goods_performance_monthly`
(
    `goods_id`         bigint COMMENT '商品id',
    `virtual_goods_id` bigint COMMENT '虚拟id',
    `project_name`     string COMMENT '组织名',
    `country_code`     string COMMENT '国家code',
    `platform_name`    string COMMENT '平台platform name in(PC,H5,APP,Others)',
    `add_uv`           bigint COMMENT '加车session数',
    `detail_add_uv`    bigint COMMENT '详情页加车数',
    `detail_view_uv`   bigint COMMENT 'view数',
    `order_num`        bigint COMMENT '已支付订单数',
    `sales_num`        bigint COMMENT '已支付销量',
    `sales_amount`     decimal(15,4) COMMENT '已支付订单销售额'
)
    COMMENT '按月商品加车,详情页展示,加车,订单，销售额'
    PARTITIONED BY (`mt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as PARQUET;