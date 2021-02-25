INSERT overwrite table ods_fd_romeo.ods_fd_order_inv_reserved
select /*+ REPARTITION(1) */order_inv_reserved_id, `version`, status, order_id, facility_id, container_id, party_id, reserved_time, delivery_time, order_time, version2, created_stamp, last_updated_stamp
from ods_fd_romeo.ods_fd_order_inv_reserved_arc
where pt = '${pt}';