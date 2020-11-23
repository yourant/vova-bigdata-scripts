set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_order_marketing_data 
select `(pt)?+.+`
from ods_fd_vb.ods_fd_order_marketing_data_arc
where pt >= '${hiveconf:pt}'
;
