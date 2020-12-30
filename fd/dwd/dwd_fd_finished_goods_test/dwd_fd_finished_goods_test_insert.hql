insert overwrite table dwd.dwd_fd_finished_goods_test
select
    /*+ REPARTITION(1) */
       gtg.id               as test_thread_id,
       gtg.goods_id,
       g.virtual_goods_id,
       gtg.pipeline_id,
       gtp.project,
       gtp.platform,
       gtp.country,
       g.cat_id,
       g.cat_name,
       gtg.state,
       gtg.type_id,
       gtg.result,
       gtg.reason,
       gtg.goods_type,
       gtg.goods_source,
       gtg.test_count,
       gtg.create_time,
       gtg.test_time,
       gtg.last_update_time as finish_time
from ods_fd_vb.ods_fd_goods_test_goods gtg
         left join ods_fd_vb.ods_fd_goods_test_pipeline gtp on gtg.pipeline_id = gtp.pipeline_id
         left join dim.dim_fd_goods g on g.goods_id = gtg.goods_id and g.project_name = gtp.project
where gtg.result != 0 and gtg.test_type=1;