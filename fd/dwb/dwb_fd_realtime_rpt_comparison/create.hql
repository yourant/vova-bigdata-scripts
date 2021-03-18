create table if not exists dwb.dwb_fd_realtime_rpt_comparison
(
    project                string COMMENT '组织',
    platform               string COMMENT '平台',
    country                string COMMENT '国家',

    session_number         bigint COMMENT 'session数',
    order_number           bigint COMMENT '订单数',
    gmv                    decimal(15, 4) COMMENT 'gmv',
    goods_amount           decimal(15, 4) comment 'goods_amount',
    conversion_rate        decimal(15, 4) COMMENT '整体转化率',

    session_number_1d_ago  bigint COMMENT '1天前同期 session数',
    order_number_1d_ago    bigint COMMENT '1天前同期 订单数',
    gmv_1d_ago             decimal(15, 4) COMMENT '1天前同期 gmv',
    goods_amount_1d_ago    decimal(15, 4) comment '1天前同期 goods_amount',
    conversion_rate_1d_ago decimal(15, 4) COMMENT '1天前同期 整体转化率',

    session_number_7d_ago  bigint COMMENT '7天前同期 session数',
    order_number_7d_ago    bigint COMMENT '7天前同期 订单数',
    gmv_7d_ago             decimal(15, 4) COMMENT '7天前同期 gmv',
    goods_amount_7d_ago    decimal(15, 4) comment '7天前同期 goods_amount',
    conversion_rate_7d_ago decimal(15, 4) COMMENT '7天前同期 整体转化率'

)
    partitioned by (pt string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS parquet
    TBLPROPERTIES ("parquet.compress" = "SNAPPY");