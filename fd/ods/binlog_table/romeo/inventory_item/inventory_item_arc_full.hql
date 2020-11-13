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
    unit_cost                   decimal(15, 4) comment '商品采购单价',
    root_inventory_item_id      string,
    parent_inventory_item_id    string,
    currency                    string
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;


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
                if(created_stamp != "0000-00-00 00:00:00" or created_stamp is not null,
                    unix_timestamp(to_utc_timestamp(created_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0)         AS created_stamp,
                if(last_updated_stamp != "0000-00-00 00:00:00" or last_updated_stamp is not null,
                    unix_timestamp(to_utc_timestamp(last_updated_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0)    AS last_updated_stamp,
                if(last_updated_tx_stamp != "0000-00-00 00:00:00" or last_updated_tx_stamp is not null,
                    unix_timestamp(to_utc_timestamp(last_updated_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_updated_tx_stamp,
                if(created_tx_stamp != "0000-00-00 00:00:00" or created_tx_stamp is not null,
                    unix_timestamp(to_utc_timestamp(created_tx_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0)      AS created_tx_stamp,
                comments,
                currency_uom_id,
                uom_id,
                owner_party_id,
                location_seq_id,
                party_id,
                if(datetime_received != "0000-00-00 00:00:00" or datetime_received is not null,
                    unix_timestamp(to_utc_timestamp(datetime_received, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0)     AS datetime_received,
                if(datetime_manufactured != "0000-00-00 00:00:00" or datetime_manufactured is not null,
                    unix_timestamp(to_utc_timestamp(datetime_manufactured, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS datetime_manufactured,
                if(expire_date != "0000-00-00 00:00:00" or expire_date is not null,
                    unix_timestamp(to_utc_timestamp(expire_date, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0)           AS expire_date,
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
        from tmp.tmp_fd_romeo_inventory_item_full

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
                    currency
                    row_number () OVER (PARTITION BY inventory_item_id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_inventory_item_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
