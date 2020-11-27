set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_fd_stock_goods_config_arc PARTITION (pt = '${pt}')
select 
     id, goods_id, min_quantity, produce_days, change_provider, change_provider_days, change_provider_reason, pms_purchase, pms_purchase_days, is_delete, update_date, fabric, provider_type, audit_submit_time, audit_action_id, statu
from (

    select 
        pt,id, goods_id, min_quantity, produce_days, change_provider, change_provider_days, change_provider_reason, pms_purchase, pms_purchase_days, is_delete, update_date, fabric, provider_type, audit_submit_time, audit_action_id, statu,
        row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (

        select  pt,
                id,
                goods_id,
                min_quantity,
                produce_days,
                change_provider,
                change_provider_days,
                change_provider_reason,
                pms_purchase,
                pms_purchase_days,
                is_delete,
                update_date,
                fabric,
                provider_type,
                audit_submit_time,
                audit_action_id,
                statu
        from ods_fd_ecshop.ods_fd_fd_stock_goods_config_arc where pt = '${pt_last}'

        UNION

        select  pt,
                id,
                goods_id,
                min_quantity,
                produce_days,
                change_provider,
                change_provider_days,
                change_provider_reason,
                pms_purchase,
                pms_purchase_days,
                is_delete,
                update_date,
                fabric,
                provider_type,
                audit_submit_time,
                audit_action_id,
                statu
        from ods_fd_ecshop.ods_fd_fd_stock_goods_config_inc where pt='${pt}'
    ) arc 
) tab where tab.rank = 1;
