CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_inc (
    `id` bigint COMMENT '',
    `bak_id` bigint COMMENT '备份id',
    `bak_order_date` timestamp COMMENT '备份订单销量截止日期',
    `external_goods_id` bigint COMMENT '商品id',
    `on_sale_time` timestamp COMMENT '商品上架时间',
    `7d_sale` decimal(13,4) COMMENT '最近七天平均销量',
    `14d_sale` decimal(13,4) COMMENT '最近14天平均销量',
    `28d_sale` decimal(13,4) COMMENT '最近28天平均销量',
    `uniq_sku` string COMMENT 'sku'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail_arc (
    `id` bigint COMMENT '',
    `bak_id` bigint COMMENT '备份id',
    `bak_order_date` timestamp COMMENT '备份订单销量截止日期',
    `external_goods_id` bigint COMMENT '商品id',
    `on_sale_time` timestamp COMMENT '商品上架时间',
    `7d_sale` decimal(13,4) COMMENT '最近七天平均销量',
    `14d_sale` decimal(13,4) COMMENT '最近14天平均销量',
    `28d_sale` decimal(13,4) COMMENT '最近28天平均销量',
    `uniq_sku` string COMMENT 'sku'
) comment '同步的近14天日销数据表'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;


CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_fd_stock_ecs_order_sale_bak_detail (
    `id` bigint COMMENT '',
    `bak_id` bigint COMMENT '备份id',
    `bak_order_date` timestamp COMMENT '备份订单销量截止日期',
    `external_goods_id` bigint COMMENT '商品id',
    `on_sale_time` timestamp COMMENT '商品上架时间',
    `7d_sale` decimal(13,4) COMMENT '最近七天平均销量',
    `14d_sale` decimal(13,4) COMMENT '最近14天平均销量',
    `28d_sale` decimal(13,4) COMMENT '最近28天平均销量',
    `uniq_sku` string COMMENT 'sku'
) comment '同步的近14天日销数据表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


