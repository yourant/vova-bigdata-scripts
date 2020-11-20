set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_order_extension
select id, order_id, ext_name, ext_value, is_delete, last_update_time
from ods_fd_vb.ods_fd_order_extension_arc 
where pt = '${hiveconf:pt}'
;

