INSERT overwrite table ods_fd_ecshop.ods_fd_ecs_region_arc PARTITION (pt = '${pt}')
select 
     region_id, parent_id, region_name, region_type, region_cn_name, region_code
from (

    select 
        pt,region_id, parent_id, region_name, region_type, region_cn_name, region_code,
        row_number () OVER (PARTITION BY region_id ORDER BY pt DESC) AS rank
    from (

        select  pt,
                region_id,
                parent_id,
                region_name,
                region_type,
                region_cn_name,
                region_code
        from ods_fd_ecshop.ods_fd_ecs_region_arc where pt = '${pt_last}'

        UNION ALL

        select  pt,
                region_id,
                parent_id,
                region_name,
                region_type,
                region_cn_name,
                region_code
        from ods_fd_ecshop.ods_fd_ecs_region_inc where pt='${pt}'
    ) arc 
) tab where tab.rank = 1;
