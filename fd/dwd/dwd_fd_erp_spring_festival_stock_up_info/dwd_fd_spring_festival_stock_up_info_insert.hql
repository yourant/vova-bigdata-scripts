insert overwrite table dwd.dwd_fd_spring_festival_stock_up_info
select
 /*+ REPARTITION(1) */
      stock_up_id
     ,goods_id
     ,small_cat
     ,season
     ,start_date
     ,end_date
     ,node_time
     ,source
     ,remark
     ,is_open
     ,open_time
     ,cast(batch_spring_ratio as decimal(10,4))
     ,cast(total_spring_ratio as decimal(10,4))
     ,status
     ,is_del
     ,created_at
     ,updated_at
from
    ods_fd_ecshop.ods_fd_fd_spring_festival_stock_up_info ;