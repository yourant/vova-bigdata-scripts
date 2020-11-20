set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_vb.ods_fd_order_info
select `(pt)?+.+` from ods_fd_vb.ods_fd_order_info_arc where pt >= '${hiveconf:pt}';
