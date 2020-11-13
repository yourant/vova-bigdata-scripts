CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_order_inv_reserverd_inventory_mapping (
    id                           bigint,
    order_inv_reserved_detail_id string,
    inventory_item_id            string,
    quantity                     int comment '数量',
    created_stamp                bigint,
    last_updated_stamp           bigint
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
TBLPROPERTIES ("parquet.compress" = "SNAPPY");

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_order_inv_reserverd_inventory_mapping
select `(dt)?+.+` from ods_fd_romeo.ods_fd_romeo_order_inv_reserverd_inventory_mapping_arc where dt = '${hiveconf:dt}';
