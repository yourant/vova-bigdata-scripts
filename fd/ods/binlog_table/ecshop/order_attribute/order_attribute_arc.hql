set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite table ods_fd_ecshop.ods_fd_order_attribute_arc PARTITION (pt = '${pt}')
select 
     attribute_id, order_id, attr_name, attr_value
from (

    select 
        pt,attribute_id, order_id, attr_name, attr_value,
        row_number () OVER (PARTITION BY attribute_id ORDER BY pt DESC) AS rank
    from (

        select  pt,
                attribute_id,
                order_id,
                attr_name,
                attr_value
        from ods_fd_ecshop.ods_fd_order_attribute_arc where pt = '${pt_last}'
        UNION
        select  pt,
                attribute_id,
                order_id,
                attr_name,
                attr_value
        from ods_fd_ecshop.ods_fd_order_attribute_inc where pt='${pt}'
    ) arc 
) tab where tab.rank = 1;
