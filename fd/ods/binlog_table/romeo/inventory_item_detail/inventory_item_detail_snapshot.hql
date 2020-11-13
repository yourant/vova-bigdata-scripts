CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_inventory_item_detail (
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
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_inventory_item_detail
select `(dt)?+.+` from ods_fd_romeo.ods_fd_romeo_inventory_item_detail_arc where dt = '${hiveconf:dt}';
