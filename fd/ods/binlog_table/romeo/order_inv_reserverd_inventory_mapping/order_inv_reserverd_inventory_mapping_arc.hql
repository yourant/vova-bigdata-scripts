alter table ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping_arc drop if exists partition (pt='$pt');

INSERT into table ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping_arc PARTITION (pt = '${pt}')
select /*+ REPARTITION(1) */arc.id, order_inv_reserved_detail_id, inventory_item_id, quantity, created_stamp, last_updated_stamp
from (
    select
         id, order_inv_reserved_detail_id, inventory_item_id, quantity, created_stamp, last_updated_stamp
    from (

        select
            pt,id, order_inv_reserved_detail_id, inventory_item_id, quantity, created_stamp, last_updated_stamp,
            row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
        from (

            select  pt,
                    id,
                    order_inv_reserved_detail_id,
                    inventory_item_id,
                    quantity,
                    created_stamp,
                    last_updated_stamp
            from ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping_arc where pt='${pt_last}'

            UNION ALL

            select  pt,
                    id,
                    order_inv_reserved_detail_id,
                    inventory_item_id,
                    quantity,
                    created_stamp,
                    last_updated_stamp
            from ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping_binlog_inc where pt='${pt}'

        ) arc
    ) tab where tab.rank = 1
) arc
left join (

    select id
    from ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping_binlog_inc
    where pt='${pt}'
    and event_type = 'delete'
    group by id

)inc on arc.id = inc.id
where inc.id is null;
