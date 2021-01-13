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
from
    ods_fd_ecshop.ods_fd_fd_spring_festival_stock_up_info where is_del=0 and is_open=1;