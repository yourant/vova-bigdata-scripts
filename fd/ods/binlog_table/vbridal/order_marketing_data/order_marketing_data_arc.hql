INSERT overwrite table ods_fd_vb.ods_fd_order_marketing_data_arc PARTITION (pt='${hiveconf:pt}')
select id, order_id, sp_session_id, created_time, last_update_time
from (
    select pt,id, order_id, sp_session_id, created_time, last_update_time,
       row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (
        select 
            pt,
            id,
            order_id,
            sp_session_id,
            created_time,
            last_update_time
        from ods_fd_vb.ods_fd_order_marketing_data_arc where pt='${hiveconf:pt_last}'
        UNION
        select
            pt,
            id,
            order_id,
            sp_session_id,
            created_time,
            last_update_time
        from ods_fd_vb.ods_fd_order_marketing_data_inc where pt = '${hiveconf:pt}'
    ) arc
)tab where tab.rank = 1;
