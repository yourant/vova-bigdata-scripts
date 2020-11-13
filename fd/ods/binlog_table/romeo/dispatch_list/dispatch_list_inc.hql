CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_dispatch_list_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
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
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_dispatch_list_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id
    ,o_raw.`table` AS event_table
    ,o_raw.type AS event_type
    ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
    ,cast(o_raw.ts AS BIGINT) AS event_date
    ,o_raw.dispatch_list_id
    ,if(o_raw.created_stamp != '0000-00-00 00:00:00', unix_timestamp(o_raw.created_stamp, "yyyy-MM-dd HH:mm:ss"), 0) AS created_stamp
    ,if(o_raw.last_update_stamp != '0000-00-00 00:00:00', unix_timestamp(o_raw.last_update_stamp, "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_stamp
    ,if(o_raw.last_update_tx_stamp != '0000-00-00 00:00:00', unix_timestamp(o_raw.last_update_tx_stamp, "yyyy-MM-dd HH:mm:ss"), 0) AS last_update_tx_stamp
    ,if(o_raw.created_tx_stamp != '0000-00-00 00:00:00', unix_timestamp(o_raw.created_tx_stamp, "yyyy-MM-dd HH:mm:ss"), 0) AS created_tx_stamp
    ,o_raw.currency
    ,o_raw.order_id
    ,o_raw.order_sn
    ,o_raw.party_id
    ,o_raw.external_order_sn
    ,o_raw.goods_sn
    ,o_raw.provider_id
    ,o_raw.goods_name
    ,o_raw.price
    ,o_raw.order_goods_id
    ,if(o_raw.due_date != '0000-00-00 00:00:00', unix_timestamp(o_raw.due_date, "yyyy-MM-dd HH:mm:ss"), 0) AS due_date
    ,if(o_raw.shipping_date != '0000-00-00 00:00:00', unix_timestamp(o_raw.shipping_date, "yyyy-MM-dd HH:mm:ss"), 0) AS shipping_date
    ,o_raw.dispatch_sequence_no
    ,o_raw.image_url
    ,o_raw.dispatch_sn
    ,o_raw.dispatch_priority_id
    ,o_raw.dispatch_status_id
    ,if(o_raw.submit_date != '0000-00-00 00:00:00', unix_timestamp(o_raw.submit_date, "yyyy-MM-dd HH:mm:ss"), 0) AS submit_date
    ,o_raw.purchase_order_id
    ,o_raw.purchase_order_sn
    ,if(o_raw.finished_date != '0000-00-00 00:00:00', unix_timestamp(o_raw.finished_date, "yyyy-MM-dd HH:mm:ss"), 0) AS finished_date
    ,o_raw.external_goods_id
    ,o_raw.external_cat_id
    ,hour as hour
from tmp.tmp_fd_romeo_dispatch_list
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'dispatch_list_id', 'created_stamp', 'last_update_stamp', 'created_tx_stamp', 'last_update_tx_stamp', 'currency', 'order_id', 'order_sn', 'party_id', 'external_order_sn', 'goods_sn', 'provider_id', 'goods_name', 'price', 'order_goods_id', 'due_date', 'shipping_date', 'dispatch_sequence_no', 'image_url', 'dispatch_sn', 'dispatch_priority_id', 'dispatch_status_id', 'submit_date', 'purchase_order_id', 'purchase_order_sn', 'finished_date', 'external_goods_id', 'external_cat_id') o_raw
AS `table`, ts, `commit`, xid, type, old, dispatch_list_id, created_stamp, last_update_stamp, created_tx_stamp, last_update_tx_stamp, currency, order_id, order_sn, party_id, external_order_sn, goods_sn, provider_id, goods_name, price, order_goods_id, due_date, shipping_date, dispatch_sequence_no, image_url, dispatch_sn, dispatch_priority_id, dispatch_status_id, submit_date, purchase_order_id, purchase_order_sn, finished_date, external_goods_id, external_cat_id
where dt = '${hiveconf:dt}';
