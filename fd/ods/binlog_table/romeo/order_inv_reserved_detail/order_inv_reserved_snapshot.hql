CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_order_inv_reserved_detail (
    order_inv_reserved_detail_id string,
    status                       string,
    order_id                     bigint,
    order_item_id                bigint,
    goods_number                 bigint,
    product_id                   bigint,
    order_inv_reserved_id        string,
    reserved_quantity            bigint,
    reserved_time                bigint,
    status_id                    string,
    facility_id                  bigint,
    version                      bigint,
    created_stamp                bigint,
    last_updated_stamp           bigint
) COMMENT '来自对应arc表的数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_order_inv_reserved_detail
select `(dt)?+.+` from ods_fd_romeo.ods_fd_romeo_order_inv_reserved_detail_arc where dt = '${hiveconf:dt}';
