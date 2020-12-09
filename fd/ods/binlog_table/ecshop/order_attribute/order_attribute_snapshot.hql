set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ecshop.ods_fd_order_attribute
select attribute_id, order_id, attr_name, attr_value
from ods_fd_ecshop.ods_fd_order_attribute_arc
where pt = '${pt}';
