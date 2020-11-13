CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_inventory_item_arc (
    inventory_item_id           string,
    serial_number               bigint,
    status_id                   string,
    inventory_item_acct_type_id string,
    inventory_item_type_id      string,
    facility_id                 bigint,
    container_id                string,
    quantity_on_hand_total      bigint,
    available_to_promise        bigint,
    available_to_promise_total  bigint,
    quantity_on_hand            bigint,
    product_id                  bigint,
    created_stamp               bigint,
    last_updated_stamp          bigint,
    last_updated_tx_stamp       bigint,
    created_tx_stamp            bigint,
    comments                    string,
    currency_uom_id             bigint,
    uom_id                      bigint,
    owner_party_id              bigint,
    location_seq_id             bigint,
    party_id                    bigint,
    datetime_received           bigint,
    datetime_manufactured       bigint,
    expire_date                 bigint,
    lot_id                      bigint,
    bin_number                  bigint,
    soft_identifier             bigint,
    activation_number           bigint,
    activation_valid_thru       string,
    provider_id                 bigint comment '采购订单供应商id',
    unit_cost                   decimal(15, 6) comment '商品采购单价',
    root_inventory_item_id      string,
    parent_inventory_item_id    string,
    currency                    string
) COMMENT '来自kafka erp romeo_inventory_item数据'
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_inventory_item_arc PARTITION (dt = '${hiveconf:dt}')
select 
     inventory_item_id, serial_number, status_id, inventory_item_acct_type_id, inventory_item_type_id, facility_id, container_id, quantity_on_hand_total, available_to_promise, available_to_promise_total, quantity_on_hand, product_id, created_stamp, last_updated_stamp, last_updated_tx_stamp, created_tx_stamp, comments, currency_uom_id, uom_id, owner_party_id, location_seq_id, party_id, datetime_received, datetime_manufactured, expire_date, lot_id, bin_number, soft_identifier, activation_number, activation_valid_thru, provider_id, unit_cost, root_inventory_item_id, parent_inventory_item_id, currency
from (

    select 
        dt,inventory_item_id, serial_number, status_id, inventory_item_acct_type_id, inventory_item_type_id, facility_id, container_id, quantity_on_hand_total, available_to_promise, available_to_promise_total, quantity_on_hand, product_id, created_stamp, last_updated_stamp, last_updated_tx_stamp, created_tx_stamp, comments, currency_uom_id, uom_id, owner_party_id, location_seq_id, party_id, datetime_received, datetime_manufactured, expire_date, lot_id, bin_number, soft_identifier, activation_number, activation_valid_thru, provider_id, unit_cost, root_inventory_item_id, parent_inventory_item_id, currency,
        row_number () OVER (PARTITION BY inventory_item_id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt,
                inventory_item_id,
                serial_number,
                status_id,
                inventory_item_acct_type_id,
                inventory_item_type_id,
                facility_id,
                container_id,
                quantity_on_hand_total,
                available_to_promise,
                available_to_promise_total,
                quantity_on_hand,
                product_id,
                created_stamp,
                last_updated_stamp,
                last_updated_tx_stamp,
                created_tx_stamp,
                comments,
                currency_uom_id,
                uom_id,
                owner_party_id,
                location_seq_id,
                party_id,
                datetime_received,
                datetime_manufactured,
                expire_date,
                lot_id,
                bin_number,
                soft_identifier,
                activation_number,
                activation_valid_thru,
                provider_id,
                unit_cost,
                root_inventory_item_id,
                parent_inventory_item_id,
                currency
        from ods_fd_romeo.ods_fd_romeo_inventory_item_arc where dt='${hiveconf:dt_last}'
        UNION
        select dt,inventory_item_id, serial_number, status_id, inventory_item_acct_type_id, inventory_item_type_id, facility_id, container_id, quantity_on_hand_total, available_to_promise, available_to_promise_total, quantity_on_hand, product_id, created_stamp, last_updated_stamp, last_updated_tx_stamp, created_tx_stamp, comments, currency_uom_id, uom_id, owner_party_id, location_seq_id, party_id, datetime_received, datetime_manufactured, expire_date, lot_id, bin_number, soft_identifier, activation_number, activation_valid_thru, provider_id, unit_cost, root_inventory_item_id, parent_inventory_item_id, currency
        from (

            select  '2020-09-24' as dt,
                    inventory_item_id,
                    serial_number,
                    status_id,
                    inventory_item_acct_type_id,
                    inventory_item_type_id,
                    facility_id,
                    container_id,
                    quantity_on_hand_total,
                    available_to_promise,
                    available_to_promise_total,
                    quantity_on_hand,
                    product_id,
                    created_stamp,
                    last_updated_stamp,
                    last_updated_tx_stamp,
                    created_tx_stamp,
                    comments,
                    currency_uom_id,
                    uom_id,
                    owner_party_id,
                    location_seq_id,
                    party_id,
                    datetime_received,
                    datetime_manufactured,
                    expire_date,
                    lot_id,
                    bin_number,
                    soft_identifier,
                    activation_number,
                    activation_valid_thru,
                    provider_id,
                    unit_cost,
                    root_inventory_item_id,
                    parent_inventory_item_id,
                    currency,
                    row_number () OVER (PARTITION BY inventory_item_id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_inventory_item_inc where dt='${hiveconf:dt}'
        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
