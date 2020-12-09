alter table ods_fd_ecshop.ods_fd_fd_stock_ecs_order_sale_bak_detail_arc drop if exists partition (pt='$pt');

INSERT into table ods_fd_ecshop.ods_fd_fd_stock_ecs_order_sale_bak_detail_arc PARTITION (pt = '${pt}')
select 
     id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku
from (

    select 
        pt,id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku,
        row_number () OVER (PARTITION BY id ORDER BY pt DESC) AS rank
    from (

        select  pt,
                id,
                bak_id,
                bak_order_date,
                external_goods_id,
                on_sale_time,
                7d_sale,
                14d_sale,
                28d_sale,
                uniq_sku
        from ods_fd_ecshop.ods_fd_fd_stock_ecs_order_sale_bak_detail_arc where pt = '${pt_last}'

        UNION ALL

        select  pt,
                id,
                bak_id,
                bak_order_date,
                external_goods_id,
                on_sale_time,
                7d_sale,
                14d_sale,
                28d_sale,
                uniq_sku
        from ods_fd_ecshop.ods_fd_fd_stock_ecs_order_sale_bak_detail_inc where pt='${pt}'
    )  arc 
) tab where tab.rank = 1;
