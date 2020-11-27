set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ecshop.ods_fd_fd_sku_backups
select id,uniq_sku,sale_region,color,size
from ods_fd_ecshop.ods_fd_fd_sku_backups_arc
where pt = '${pt}';
