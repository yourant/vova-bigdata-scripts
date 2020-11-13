CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_goods (
    rec_id                  bigint comment '订单商品记录id(unique)',
    order_id                bigint,
    goods_id                bigint,
    goods_name              string,
    goods_sn                string,
    goods_number            bigint,
    market_price            decimal(10, 2),
    goods_price             decimal(16, 6),
    goods_attr              string,
    send_number             bigint,
    is_real                 tinyint,
    extension_code          string,
    parent_id               bigint,
    is_gift                 bigint,
    goods_status            tinyint,
    action_amt              decimal(10, 2),
    action_reason_cat       tinyint,
    action_note             string,
    carrier_bill_id         bigint,
    provider_id             bigint,
    invoice_num             string,
    return_points           bigint,
    return_bonus            string,
    biaoju_store_goods_id   bigint,
    subtitle                string,
    addtional_shipping_fee  bigint,
    style_id                bigint,
    customized              string,
    status_id               string comment '商品新旧状态',
    added_fee               decimal(10, 4) comment '税率',
    external_order_goods_id bigint comment '网站order_goods_id'
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_order_goods
select `(dt)?+.+` from ods_fd_ecshop.ods_fd_ecs_order_goods_arc where dt = '${hiveconf:dt}';
