CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_inventory_item_detail (
    -- maxwell event data
    event_id STRING,
    event_table STRING,
    event_type STRING,
    event_commit BOOLEAN,
    event_date BIGINT,
    -- now data
    inventory_item_detail_id  string,
    description               string,
    inventory_item_id         string,
    physical_inventory_id     string,
    created_stamp             bigint,
    last_updated_stamp        bigint,   
    last_updated_tx_stamp     bigint,
    created_tx_stamp          bigint,
    quantity_on_hand_diff     bigint,
    available_to_promise_diff bigint,
    order_id                  bigint,
    order_item_seq_id         bigint,
    ship_group_seq_id         bigint,
    shipment_id               bigint,
    shipment_item_seq_id      bigint,
    work_effort_id            bigint,
    fixed_asset_id            bigint,
    maint_hist_seq_id         bigint,
    item_issuance_id          bigint,
    receipt_id                bigint,
    reason_enum_id            bigint,
    cancellation_flag         string,
    inventory_transaction_id  string,
    order_goods_id            bigint,
    is_purchase_confirm       bigint comment '采购已确认对账单',
    is_finance_confirm        bigint comment '财务已付款对账单'
) COMMENT '来自kafka erp表每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_inventory_item_detail_inc  PARTITION (dt='${hiveconf:dt}',hour)
select 
    o_raw.xid AS event_id
    ,o_raw.`table` AS event_table
    ,o_raw.type AS event_type
    ,cast(o_raw.`commit` AS BOOLEAN) AS event_commit
    ,cast(o_raw.ts AS BIGINT) AS event_date
    ,o_raw.inventory_item_detail_id,
    ,o_raw.description,
    ,o_raw.inventory_item_id,
    ,o_raw.physical_inventory_id,
    ,if(o_raw.created_stamp != "0000-00-00 00:00:00" or o_raw.created_stamp is not null,
    unix_timestamp(o_raw.created_stamp, "yyyy-MM-dd HH:mm:ss"), 0)         AS created_stamp
    ,if(o_raw.last_updated_stamp != "0000-00-00 00:00:00" or o_raw.last_updated_stamp is not null,
    unix_timestamp(o_raw.last_updated_stamp, "yyyy-MM-dd HH:mm:ss"), 0)    AS expire_date
    ,if(o_raw.last_updated_tx_stamp != "0000-00-00 00:00:00" or o_raw.last_updated_tx_stamp is not null,
    unix_timestamp(o_raw.last_updated_tx_stamp, "yyyy-MM-dd HH:mm:ss"), 0) AS last_updated_tx_stamp
    ,if(o_raw.created_tx_stamp != "0000-00-00 00:00:00" or o_raw.created_tx_stamp is not null,
    unix_timestamp(o_raw.created_tx_stamp, "yyyy-MM-dd HH:mm:ss"), 0)      AS created_tx_stamp
    ,o_raw.quantity_on_hand_diff
    ,o_raw.available_to_promise_diff
    ,o_raw.order_id
    ,o_raw.order_item_seq_id
    ,o_raw.ship_group_seq_id
    ,o_raw.shipment_id
    ,o_raw.shipment_item_seq_id
    ,o_raw.work_effort_id
    ,o_raw.fixed_asset_id
    ,o_raw.maint_hist_seq_id
    ,o_raw.item_issuance_id
    ,o_raw.receipt_id
    ,o_raw.reason_enum_id
    ,o_raw.cancellation_flag
    ,o_raw.inventory_transaction_id
    ,o_raw.order_goods_id
    ,o_raw.is_purchase_confirm
    ,o_raw.is_finance_confir
    ,hour as hour
from tmp.tmp_fd_romeo_party
LATERAL VIEW json_tuple(value, 'kafka_table', 'kafka_ts', 'kafka_commit', 'kafka_xid','kafka_type' , 'kafka_old' , 'inventory_item_detail_id', 'description', 'inventory_item_id', 'physical_inventory_id', 'created_stamp', 'last_updated_stamp', 'last_updated_tx_stamp', 'created_tx_stamp', 'quantity_on_hand_diff', 'available_to_promise_diff', 'order_id', 'order_item_seq_id', 'ship_group_seq_id', 'shipment_id', 'shipment_item_seq_id', 'work_effort_id', 'fixed_asset_id', 'maint_hist_seq_id', 'item_issuance_id', 'receipt_id', 'reason_enum_id', 'cancellation_flag', 'inventory_transaction_id', 'order_goods_id', 'is_purchase_confirm', 'is_finance_confirm') o_raw
AS `table`, ts, `commit`, xid, type, old, inventory_item_detail_id, description, inventory_item_id, physical_inventory_id, created_stamp, last_updated_stamp, last_updated_tx_stamp, created_tx_stamp, quantity_on_hand_diff, available_to_promise_diff, order_id, order_item_seq_id, ship_group_seq_id, shipment_id, shipment_item_seq_id, work_effort_id, fixed_asset_id, maint_hist_seq_id, item_issuance_id, receipt_id, reason_enum_id, cancellation_flag, inventory_transaction_id, order_goods_id, is_purchase_confirm, is_finance_confirm
where dt = '${hiveconf:dt}';
