CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_inventory_item (
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
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_inventory_item
select `(dt)?+.+` from ods_fd_romeo.ods_fd_romeo_inventory_item_arc where dt = '${hiveconf:dt}';
