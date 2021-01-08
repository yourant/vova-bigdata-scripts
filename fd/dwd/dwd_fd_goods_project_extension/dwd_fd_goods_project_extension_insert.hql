insert overwrite table dwd.dwd_fd_goods_project_extension
select
      /*+ REPARTITION(1) */
       id
      ,goods_id
      ,project_name
      ,ext_name
      ,ext_value
      ,created
      ,modified
from ods_fd_vb.ods_fd_goods_project_extension ;