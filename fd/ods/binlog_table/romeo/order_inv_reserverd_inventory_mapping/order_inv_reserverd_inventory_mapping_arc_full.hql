CREATE TABLE IF NOT EXISTS ods_fd_romeo.ods_fd_romeo_order_inv_reserverd_inventory_mapping_arc (
    id                           bigint,
    order_inv_reserved_detail_id string,
    inventory_item_id            string,
    quantity                     bigint comment '数量',
    created_stamp                bigint,
    last_updated_stamp           bigint
) COMMENT '来自kafka erp订单每日增量数据'
PARTITIONED BY (dt STRING,hour STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS PARQUETFILE
;


set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_romeo.ods_fd_romeo_order_inv_reserverd_inventory_mapping_arc PARTITION (dt = '${hiveconf:dt}')
select 
     id, order_inv_reserved_detail_id, inventory_item_id, quantity, created_stamp, last_updated_stamp
from (

    select 
        dt, id, order_inv_reserved_detail_id, inventory_item_id, quantity, created_stamp, last_updated_stamp,
        row_number () OVER (PARTITION BY order_inv_reserved_detail_id ORDER BY dt DESC) AS rank
    from (

        select  '2020-01-01' as dt
                id,
                order_inv_reserved_detail_id,
                inventory_item_id,
                quantity,
                if(created_stamp != "0000-00-00 00:00:00" or created_stamp is not null,
                    unix_timestamp(to_utc_timestamp(created_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS created_stamp,
                if(last_updated_stamp != "0000-00-00 00:00:00" or last_updated_stamp is not null,
                    unix_timestamp(to_utc_timestamp(last_updated_stamp, "Asia/Shanghai"), "yyyy-MM-dd HH:mm:ss"), 0) AS last_updated_stamp
        from tmp.tmp_fd_romeo_order_inv_reserverd_inventory_mapping_full

        UNION

        select dt,order_inv_reserved_detail_id, status, order_id, order_item_id, goods_number, product_id, order_inv_reserved_id, reserved_quantity, reserved_time, status_id, facility_id, version, created_stamp, last_updated_stamp
        from (

            select  '2020-09-24' as dt,
                    id,
                    order_inv_reserved_detail_id,
                    inventory_item_id,
                    quantity,
                    created_stamp,
                    last_updated_stamp,
                    row_number () OVER (PARTITION BY id ORDER BY event_id DESC) AS rank
            from ods_fd_romeo.ods_fd_romeo_order_inv_reserverd_inventory_mapping_inc where dt='${hiveconf:dt}'

        ) inc where inc.rank = 1
    ) arc 
) tab where tab.rank = 1;
