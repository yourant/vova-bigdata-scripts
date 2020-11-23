CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_dispatch_list_arc (
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
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (pt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_dispatch_list_arc PARTITION (pt = '${hiveconf:pt}')
select 
     dispatch_list_id, created_stamp, last_update_stamp, created_tx_stamp, last_update_tx_stamp, currency, order_id, order_sn, party_id, external_order_sn, goods_sn, provider_id, goods_name, price, order_goods_id, due_date, shipping_date, dispatch_sequence_no, image_url, dispatch_sn, dispatch_priority_id, dispatch_status_id, submit_date, purchase_order_id, purchase_order_sn, finished_date, external_goods_id, external_cat_id
from (

    select 
        pt,dispatch_list_id, created_stamp, last_update_stamp, created_tx_stamp, last_update_tx_stamp, currency, order_id, order_sn, party_id, external_order_sn, goods_sn, provider_id, goods_name, price, order_goods_id, due_date, shipping_date, dispatch_sequence_no, image_url, dispatch_sn, dispatch_priority_id, dispatch_status_id, submit_date, purchase_order_id, purchase_order_sn, finished_date, external_goods_id, external_cat_id,
        row_number () OVER (PARTITION BY dispatch_list_id ORDER BY pt DESC) AS rank
    from (

        select  pt
                dispatch_list_id,
                created_stamp,
                last_update_stamp,
                created_tx_stamp,
                last_update_tx_stamp,
                currency,
                order_id,
                order_sn,
                party_id,
                external_order_sn,
                goods_sn,
                provider_id,
                goods_name,
                price,
                order_goods_id,
                due_date,
                shipping_date,
                dispatch_sequence_no,
                image_url,
                dispatch_sn,
                dispatch_priority_id,
                dispatch_status_id,
                submit_date,
                purchase_order_id,
                purchase_order_sn,
                finished_date,
                external_goods_id,
                external_cat_id
        from ods_fd_romeo.ods_fd_romeo_dispatch_list_arc where pt = '${hiveconf:pt_last}'

        UNION

        select pt,dispatch_list_id, created_stamp, last_update_stamp, created_tx_stamp, last_update_tx_stamp, currency, order_id, order_sn, party_id, external_order_sn, goods_sn, provider_id, goods_name, price, order_goods_id, due_date, shipping_date, dispatch_sequence_no, image_url, dispatch_sn, dispatch_priority_id, dispatch_status_id, submit_date, purchase_order_id, purchase_order_sn, finished_date, external_goods_id, external_cat_id
        from (

            select  pt
                    dispatch_list_id,
                    created_stamp,
                    last_update_stamp,
                    created_tx_stamp,
                    last_update_tx_stamp,
                    currency,
                    order_id,
                    order_sn,
                    party_id,
                    external_order_sn,
                    goods_sn,
                    provider_id,
                    goods_name,
                    price,
                    order_goods_id,
                    due_date,
                    shipping_date,
                    dispatch_sequence_no,
                    image_url,
                    dispatch_sn,
                    dispatch_priority_id,
                    dispatch_status_id,
                    submit_date,
                    purchase_order_id,
                    purchase_order_sn,
                    finished_date,
                    external_goods_id,
                    external_cat_id,
                    row_number () OVER (PARTITION BY dispatch_list_id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_dispatch_list_inc where pt='${hiveconf:pt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
