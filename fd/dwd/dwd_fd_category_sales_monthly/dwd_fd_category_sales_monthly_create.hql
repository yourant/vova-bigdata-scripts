CREATE TABLE IF NOT EXISTS `dwd.dwd_fd_category_sales_monthly`
(
    `cat_id`        bigint COMMENT '分类id',
    `cat_name`      string COMMENT '分类名字',
    `project_name`  string COMMENT '组织名',
    `platform_name` string COMMENT '平台platform type in(PC,H5,APP,Others)',
    `country_code`  string COMMENT '国家code',
    `order_num`     bigint COMMENT '已支付订单数',
    `sales_num`     bigint COMMENT '已支付销量',
    `sales_amount`  decimal(15,4) COMMENT '已支付订单销售额'
)
    COMMENT '按月的国家品类已支付销量和销售额'
    PARTITIONED BY (`mt` string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    stored as parquet;