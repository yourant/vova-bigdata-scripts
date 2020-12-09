INSERT overwrite table ods_fd_vb.ods_fd_order_status_change_history_arc PARTITION (pt='${hiveconf:pt}')
select id, order_sn, field_name, old_value, new_value, create_time
from (
        select pt, id, order_sn, field_name, old_value, new_value, create_time,
        row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (
        select
            pt,
            id,
            order_sn,
            field_name,
            old_value,
            new_value,
            create_time
        from ods_fd_vb.ods_fd_order_status_change_history_arc where pt='${hiveconf:pt_last}'

        UNION
        select
            pt,
            id,
            order_sn,
            field_name,
            old_value,
            new_value,
            create_time
        from ods_fd_vb.ods_fd_order_status_change_history_inc where pt = '${hiveconf:pt}'
    )arc

) tab where tab.rank = 1;