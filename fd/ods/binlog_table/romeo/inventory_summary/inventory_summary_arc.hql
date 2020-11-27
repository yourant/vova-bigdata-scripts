INSERT overwrite table ods_fd_romeo.ods_fd_inventory_summary_arc PARTITION (pt = '${pt}')
select 
     inventory_summary_id,status_id,facility_id,container_id,product_id,stock_quantity,available_to_reserved,
demand_quantity,making_quantity,created_stamp,last_updated_stamp,last_updated_tx_stamp,created_tx_stamp,currency_uom_id,comments,uom_id,owner_party_id,party_id,unit_cost
from (

    select 
        pt,inventory_summary_id,status_id,facility_id,container_id,product_id,stock_quantity,available_to_reserved,
demand_quantity,making_quantity,created_stamp,last_updated_stamp,last_updated_tx_stamp,created_tx_stamp,currency_uom_id,comments,uom_id,owner_party_id,party_id,unit_cost,
        row_number () OVER (PARTITION BY inventory_summary_id ORDER BY pt DESC) AS rank
    from (

        select  pt,
                inventory_summary_id,
                status_id,
                facility_id,
                container_id,
                product_id,
                stock_quantity,
                available_to_reserved,
                demand_quantity,
                making_quantity,
                created_stamp,
                last_updated_stamp,
                last_updated_tx_stamp,
                created_tx_stamp,
                currency_uom_id,
                comments,
                uom_id,
                owner_party_id,
                party_id,
                unit_cost
        from ods_fd_romeo.ods_fd_inventory_summary_arc where pt='${pt_last}'

        UNION ALL

        select  pt,
                inventory_summary_id,
                status_id,
                facility_id,
                container_id,
                product_id,
                stock_quantity,
                available_to_reserved,
                demand_quantity,
                making_quantity,
                created_stamp,
                last_updated_stamp,
                last_updated_tx_stamp,
                created_tx_stamp,
                currency_uom_id,
                comments,
                uom_id,
                owner_party_id,
                party_id,
                unit_cost
        from ods_fd_romeo.ods_fd_inventory_summary_inc where pt='${pt}'
    ) arc 
) tab where tab.rank = 1;
