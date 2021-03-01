alter table ods_fd_romeo.ods_fd_order_inv_reserved_detail_arc drop if exists partition (pt='$pt');

INSERT into table ods_fd_romeo.ods_fd_order_inv_reserved_detail_arc PARTITION (pt = '${pt}')
select /*+ REPARTITION(1) */arc.order_inv_reserved_detail_id, status, order_id, order_item_id, goods_number, product_id, order_inv_reserved_id, reserved_quantity, reserved_time, status_id, facility_id, version, created_stamp, last_updated_stamp
from(
    select
         order_inv_reserved_detail_id, status, order_id, order_item_id, goods_number, product_id, order_inv_reserved_id, reserved_quantity, reserved_time, status_id, facility_id, version, created_stamp, last_updated_stamp
    from (

        select
            pt,order_inv_reserved_detail_id, status, order_id, order_item_id, goods_number, product_id, order_inv_reserved_id, reserved_quantity, reserved_time, status_id, facility_id, version, created_stamp, last_updated_stamp,
            row_number () OVER (PARTITION BY order_inv_reserved_detail_id ORDER BY pt DESC) AS rank
        from (

            select  pt,
                    order_inv_reserved_detail_id,
                    status,
                    order_id,
                    order_item_id,
                    goods_number,
                    product_id,
                    order_inv_reserved_id,
                    reserved_quantity,
                    reserved_time,
                    status_id,
                    facility_id,
                    version,
                    created_stamp,
                    last_updated_stamp
            from ods_fd_romeo.ods_fd_order_inv_reserved_detail_arc where pt='${pt_last}'

            UNION

            select  pt,
                    order_inv_reserved_detail_id,
                    status,
                    order_id,
                    order_item_id,
                    goods_number,
                    product_id,
                    order_inv_reserved_id,
                    reserved_quantity,
                    reserved_time,
                    status_id,
                    facility_id,
                    version,
                    created_stamp,
                    last_updated_stamp
            from ods_fd_romeo.ods_fd_order_inv_reserved_detail_binlog_inc where pt='${pt}'

        ) arc
    ) tab where tab.rank = 1
)arc
left join (

    select order_inv_reserved_detail_id
    from ods_fd_romeo.ods_fd_order_inv_reserved_detail_binlog_inc
    where pt='${pt}'
    and event_type = 'delete'
    group by order_inv_reserved_detail_id

)inc on arc.order_inv_reserved_detail_id = inc.order_inv_reserved_detail_id
where inc.order_inv_reserved_detail_id is null;

