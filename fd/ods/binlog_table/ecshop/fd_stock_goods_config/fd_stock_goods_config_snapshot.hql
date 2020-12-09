set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ecshop.ods_fd_fd_stock_goods_config
select id, goods_id, min_quantity, produce_days, change_provider, change_provider_days, change_provider_reason, pms_purchase, pms_purchase_days, is_delete, update_date, fabric, provider_type, audit_submit_time, audit_action_id, status
from ods_fd_ecshop.ods_fd_fd_stock_goods_config_arc
where pt = '${pt}';
