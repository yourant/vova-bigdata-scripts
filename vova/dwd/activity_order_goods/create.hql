drop table if exists dwd.dwd_vova_fact_act_ord_gs;
create table dwd.dwd_vova_fact_act_ord_gs
(
    datasource        string comment '数据站点',
    act_id            bigint comment '活动id',
    act_name          string comment '活动名',
    uiq_vtl_ord_id    string comment '唯一虚拟订单id',
    vtl_ord_id        bigint comment '虚拟订单id',
    uiq_vtl_ord_gs_id string comment '唯一虚拟子订单id',
    vtl_ord_gs_id     bigint comment '虚拟子订单id',
    ord_id            bigint comment '真实order_id',
    ord_gs_id         bigint comment '真实order_goods_id',
    byr_id            bigint comment 'buyer_id',
    pmt_id            bigint comment 'payment_id',
    ord_sts           bigint comment 'order_status订单状态',
    pay_sts           bigint comment 'pay_status 支付状态',
    pay_time          timestamp comment '支付时间',
    ship_fee          decimal(10, 4) comment 'shipping_fee 运费',
    bonus             decimal(10, 4) comment '红包',
    ord_amt           decimal(10, 4) comment '订单实付金额',
    gs_cnt            bigint comment 'goods_number',
    gs_id             bigint comment 'goods_id',
    sku_id            bigint comment 'sku_id',
    ord_time          timestamp comment 'order_time',
    rcv_time          timestamp comment 'receive_time',
    sm_id             bigint comment '集运id',
    rgn_code          string comment 'region_code',
    dvc_id            string comment 'device_id',
    platform          string comment '平台',
    gender            string comment '性别',
    last_update_time  timestamp

) COMMENT '活动订单' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' STORED AS PARQUETFILE;

