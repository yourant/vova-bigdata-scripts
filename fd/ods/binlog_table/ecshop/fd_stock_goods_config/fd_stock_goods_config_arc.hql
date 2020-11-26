set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_fd_stock_goods_config_arc PARTITION (pt = '${hiveconf:pt}')
select 
     id,goods_id,min_quantity,produce_days,change_provider,change_provider_days,change_provider_reason,is_delete,update_date,fabric,provider_type
from (

    select 
        pt,id,goods_id,min_quantity,produce_days,change_provider,change_provider_days,change_provider_reason,is_delete,update_date,fabric,provider_type,
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
                is_delete,
                update_date,
                fabric,
                provider_type
        from ods_fd_ecshop.ods_fd_fd_stock_goods_config_arc where pt = '${hiveconf:pt_last}'

        UNION

        select  pt,
                id,
                goods_id,
                min_quantity,
                produce_days,
                change_provider,
                change_provider_days,
                change_provider_reason,
                is_delete,
                update_date,
                fabric,
                provider_type
        from ods_fd_ecshop.ods_fd_fd_stock_goods_config_inc where pt='${hiveconf:pt}'
    ) arc 
) tab where tab.rank = 1;
