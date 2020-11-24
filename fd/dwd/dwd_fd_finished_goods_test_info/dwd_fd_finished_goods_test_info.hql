#关闭自动装换
set hive.auto.convert.join = false;


insert overwrite table dwd.dwd_fd_finished_goods_test_info
select t.goods_id,
       dg.virtual_goods_id,
       t.pipeline_id,
       s.project ,
       s.platform ,
       s.country,
       dg.cat_id ,
       dg.cat_name ,
       t.state,
       t.type_id,
       t.result,
       t.reason,
       t.production_reached,
       t.goods_type,
       t.goods_source,
       t.test_count,
       to_date(t.create_time) as create_time,
       to_date(t.last_update_time) as last_update_time
from (
         select goods_id,
                pipeline_id,
                state,
                type_id,
                create_time,
                result,
                reason,
                production_reached,
                goods_type,
                goods_source,
                test_count,
                last_update_time
         from ods_fd_vb.ods_fd_goods_test_goods
     ) t
         left join ods_fd_vb.ods_fd_goods_test_pipeline s on t.pipeline_id = s.pipeline_id
         left join dim.dim_fd_goods dg
                   on dg.goods_id = t.goods_id and dg.project_name = s.project;