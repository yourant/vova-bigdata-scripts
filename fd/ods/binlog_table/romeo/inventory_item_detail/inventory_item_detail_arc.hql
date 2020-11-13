CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_inventory_item_detail_arc (
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
) COMMENT '来自kafka erp currency_conversion数据'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_inventory_item_detail_arc PARTITION (dt = '${hiveconf:dt}')
select 
     inventory_item_detail_id, description, inventory_item_id, physical_inventory_id, created_stamp, last_updated_stamp, last_updated_tx_stamp, created_tx_stamp, quantity_on_hand_diff, available_to_promise_diff, order_id, order_item_seq_id, ship_group_seq_id, shipment_id, shipment_item_seq_id, work_effort_id, fixed_asset_id, maint_hist_seq_id, item_issuance_id, receipt_id, reason_enum_id, cancellation_flag, inventory_transaction_id, order_goods_id, is_purchase_confirm, is_finance_confirm
from (

    select 
        dt,inventory_item_detail_id, description, inventory_item_id, physical_inventory_id, created_stamp, last_updated_stamp, last_updated_tx_stamp, created_tx_stamp, quantity_on_hand_diff, available_to_promise_diff, order_id, order_item_seq_id, ship_group_seq_id, shipment_id, shipment_item_seq_id, work_effort_id, fixed_asset_id, maint_hist_seq_id, item_issuance_id, receipt_id, reason_enum_id, cancellation_flag, inventory_transaction_id, order_goods_id, is_purchase_confirm, is_finance_confirm,
        row_number () OVER (PARTITION BY inventory_item_detail_id ORDER BY dt DESC) AS rank
    from (

        select  dt
                inventory_item_detail_id,
                description,
                inventory_item_id,
                physical_inventory_id,
                created_stamp,
                last_updated_stamp,
                last_updated_tx_stamp,
                created_tx_stamp,
                quantity_on_hand_diff,
                available_to_promise_diff,
                order_id,
                order_item_seq_id,
                ship_group_seq_id,
                shipment_id,
                shipment_item_seq_id,
                work_effort_id,
                fixed_asset_id,
                maint_hist_seq_id,
                item_issuance_id,
                receipt_id,
                reason_enum_id,
                cancellation_flag,
                inventory_transaction_id,
                order_goods_id,
                is_purchase_confirm,
                is_finance_confirm
        from ods_fd_romeo.ods_fd_romeo_inventory_item_detail_arc where dt = '${hiveconf:dt_last}'

        UNION

        select dt,inventory_item_detail_id, description, inventory_item_id, physical_inventory_id, created_stamp, last_updated_stamp, last_updated_tx_stamp, created_tx_stamp, quantity_on_hand_diff, available_to_promise_diff, order_id, order_item_seq_id, ship_group_seq_id, shipment_id, shipment_item_seq_id, work_effort_id, fixed_asset_id, maint_hist_seq_id, item_issuance_id, receipt_id, reason_enum_id, cancellation_flag, inventory_transaction_id, order_goods_id, is_purchase_confirm, is_finance_confirm
        from (

            select  dt
                    inventory_item_detail_id,
                    description,
                    inventory_item_id,
                    physical_inventory_id,
                    created_stamp,
                    last_updated_stamp,
                    last_updated_tx_stamp,
                    created_tx_stamp,
                    quantity_on_hand_diff,
                    available_to_promise_diff,
                    order_id,
                    order_item_seq_id,
                    ship_group_seq_id,
                    shipment_id,
                    shipment_item_seq_id,
                    work_effort_id,
                    fixed_asset_id,
                    maint_hist_seq_id,
                    item_issuance_id,
                    receipt_id,
                    reason_enum_id,
                    cancellation_flag,
                    inventory_transaction_id,
                    order_goods_id,
                    is_purchase_confirm,
                    is_finance_confirm,
                    row_number () OVER (PARTITION BY inventory_item_detail_id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_inventory_item_detail_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
