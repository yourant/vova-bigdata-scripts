CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_dispatch_list (
    dispatch_list_id        string,
    created_stamp           bigint,
    last_update_stamp       bigint,
    created_tx_stamp        bigint,
    last_update_tx_stamp    bigint,
    currency                string,
    order_id                bigint,
    order_sn                string,
    party_id                string,
    external_order_sn       string,
    goods_sn                string,
    provider_id             bigint,
    goods_name              string,
    price                   decimal(15, 4) comment '工单价格',
    order_goods_id          bigint,
    due_date                bigint,
    shipping_date           bigint,
    dispatch_sequence_no    bigint,
    image_url               string,
    dispatch_sn             string,
    dispatch_priority_id    string,
    dispatch_status_id      string,
    submit_date             bigint,
    purchase_order_id       bigint,
    purchase_order_sn       string comment '关联采购订单号',
    finished_date           bigint,
    external_goods_id       bigint,
    external_cat_id         bigint comment '网站分类id'
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_dispatch_list
select `(dt)?+.+` from ods_fd_romeo.ods_fd_romeo_dispatch_list_arc where dt = '${hiveconf:dt}';
