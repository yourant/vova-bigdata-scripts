alter table ods_fd_romeo.ods_fd_order_inv_reserved_arc drop if exists partition (pt='$pt');

INSERT into table ods_fd_romeo.ods_fd_order_inv_reserved_arc PARTITION (pt = '${pt}')
select /*+ REPARTITION(1) */arc.order_inv_reserved_id, `version`, status, order_id, facility_id, container_id, party_id, reserved_time, delivery_time, order_time, version2, created_stamp, last_updated_stamp
from (
    select
         order_inv_reserved_id, `version`, status, order_id, facility_id, container_id, party_id, reserved_time, delivery_time, order_time, version2, created_stamp, last_updated_stamp
    from (

        select
            pt,order_inv_reserved_id, `version`, status, order_id, facility_id, container_id, party_id, reserved_time, delivery_time, order_time, version2, created_stamp, last_updated_stamp,
            row_number () OVER (PARTITION BY order_inv_reserved_id ORDER BY pt DESC) AS rank
        from (

            select  pt,
                    order_inv_reserved_id,
                    `version`,
                    status,
                    order_id,
                    facility_id,
                    container_id,
                    party_id,
                    reserved_time,
                    delivery_time,
                    order_time,
                    version2,
                    created_stamp,
                    last_updated_stamp
            from ods_fd_romeo.ods_fd_order_inv_reserved_arc where pt='${pt_last}'

            UNION ALL

            select  pt,
                    order_inv_reserved_id,
                    `version`,
                    status,
                    order_id,
                    facility_id,
                    container_id,
                    party_id,
                    reserved_time,
                    delivery_time,
                    order_time,
                    version2,
                    created_stamp,
                    last_updated_stamp
            from ods_fd_romeo.ods_fd_order_inv_reserved_binlog_inc where pt='${pt}'

        ) arc
    ) tab where tab.rank = 1
)arc
left join (

    select order_inv_reserved_id
    from ods_fd_romeo.ods_fd_order_inv_reserved_binlog_inc
    where pt = '${pt}'
    and event_type = 'delete'
    group by order_inv_reserved_id

)inc on arc.order_inv_reserved_id = inc.order_inv_reserved_id
where inc.order_inv_reserved_id is null;
