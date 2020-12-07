insert overwrite table dwd.dwd_fd_finished_preorder
select
       /*+ REPARTITION(1) */
       pr.preorder_plan_id,
       pp.plan_name,
       pr.goods_id,
       pr.virtual_goods_id,
       pr.project,
       pr.cat_id,
       c.cat_name,
       pr.status                  as result,
       from_unixtime(update_time) as finish_time
from ods_fd_vb.ods_fd_goods_test_preorder_result pr
         left join ods_fd_vb.ods_fd_goods_preorder_plan pp
                   on pr.preorder_plan_id = pp.id
         left join dim.dim_fd_category c on c.cat_id = pr.cat_id;