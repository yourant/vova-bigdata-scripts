CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_order_inv_reserved (
    order_inv_reserved_id string,
    `version`             string,
    status                string,
    order_id              bigint,
    facility_id           string,
    container_id          string,
    party_id              string,
    reserved_time         bigint,
    order_time            bigint
) comment ''
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE;

set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_order_inv_reserved
select `(dt)?+.+` from ods_fd_romeo.ods_fd_romeo_order_inv_reserved_arc where dt = '${hiveconf:dt}';
