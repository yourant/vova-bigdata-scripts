alter table ods_fd_ecshop.ods_fd_fd_sku_backups_arc drop if exists partition (pt='$pt');

INSERT into table ods_fd_ecshop.ods_fd_fd_sku_backups_arc PARTITION (pt='$pt')
select 
     id,uniq_sku,sale_region,color,size
from (
    select 
        pt,id,uniq_sku,sale_region,color,size,
        row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (

        select  pt,
                id,
                uniq_sku,
                sale_region,
                color,
                size
        from ods_fd_ecshop.ods_fd_fd_sku_backups_arc where pt = '${pt_last}'

        UNION ALL

        select  pt,
                id,
                uniq_sku,
                sale_region,
                color,
                size
        from ods_fd_ecshop.ods_fd_fd_sku_backups_inc where pt='${pt}'

    ) arc 
) tab where tab.rank = 1;
