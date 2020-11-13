CREATE TABLE IF NOT EXISTS ods_fd_ecshop.ods_fd_ecs_order_goods_inc (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
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
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY")
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_order_goods_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id
    ,o_raw.`table` AS event_table
    ,o_raw.type AS event_type
    ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
    ,cast(o_raw.ts AS BIGINT) AS event_date
    ,o_raw.rec_id
    ,o_raw.order_id
    ,o_raw.goods_id
    ,o_raw.goods_name
    ,o_raw.goods_sn
    ,o_raw.goods_number
    ,o_raw.market_price
    ,o_raw.goods_price
    ,o_raw.goods_attr
    ,o_raw.send_number
    ,o_raw.is_real
    ,o_raw.extension_code
    ,o_raw.parent_id
    ,o_raw.is_gift
    ,o_raw.goods_status
    ,o_raw.action_amt
    ,o_raw.action_reason_cat
    ,o_raw.action_note
    ,o_raw.carrier_bill_id
    ,o_raw.provider_id
    ,o_raw.invoice_num
    ,o_raw.return_points
    ,o_raw.return_bonus
    ,o_raw.biaoju_store_goods_id
    ,o_raw.subtitle
    ,o_raw.addtional_shipping_fee
    ,o_raw.style_id
    ,o_raw.customized
    ,o_raw.status_id
    ,o_raw.added_fee
    ,o_raw.external_order_goods_id
    ,hour as hour
from tmp.tmp_fd_ecs_order_goods
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'rec_id', 'order_id', 'goods_id', 'goods_name', 'goods_sn', 'goods_number', 'market_price', 'goods_price', 'goods_attr', 'send_number', 'is_real', 'extension_code', 'parent_id', 'is_gift', 'goods_status', 'action_amt', 'action_reason_cat', 'action_note', 'carrier_bill_id', 'provider_id', 'invoice_num', 'return_points', 'return_bonus', 'biaoju_store_goods_id', 'subtitle', 'addtional_shipping_fee', 'style_id', 'customized', 'status_id', 'added_fee', 'external_order_goods_id') o_raw
AS `table`, ts, `commit`, xid, type, old, rec_id, order_id, goods_id, goods_name, goods_sn, goods_number, market_price, goods_price, goods_attr, send_number, is_real, extension_code, parent_id, is_gift, goods_status, action_amt, action_reason_cat, action_note, carrier_bill_id, provider_id, invoice_num, return_points, return_bonus, biaoju_store_goods_id, subtitle, addtional_shipping_fee, style_id, customized, status_id, added_fee, external_order_goods_i
where dt = '${hiveconf:dt}';
