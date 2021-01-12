create table if not exists dwd.dwd_fd_refund_executed
(
    refund_id              bigint comment '退款记录id',
    ecs_order_id           bigint comment '退款对应的ecs订单id',
    party_id               bigint comment '组织id',
    project                string comment '组织名称',
    total_refund_amount    DECIMAL(15, 4) comment '本次退款总金额',
    goods_refund_amount    DECIMAL(15, 4) comment '本次退款商品金额',
    shipping_refund_amount DECIMAL(15, 4) comment '本次退款运费金额',
    execute_time           timestamp comment '退款操作时间'
) comment '已完成的退款'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
    STORED AS PARQUET;