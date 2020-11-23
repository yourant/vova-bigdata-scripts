set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_order_status_change_history
select `(pt)?+.+` from ods_fd_vb.ods_fd_order_status_change_history_arc
where pt >= '${hiveconf:pt}'
;