INSERT overwrite table ods_fd_romeo.ods_fd_order_inv_reserved_detail
select /*+ REPARTITION(1) */order_inv_reserved_detail_id, status, order_id, order_item_id, goods_number, product_id, order_inv_reserved_id, reserved_quantity, reserved_time, status_id, facility_id, version, created_stamp, last_updated_stamp
from ods_fd_romeo.ods_fd_order_inv_reserved_detail_arc
where pt = '${pt}';
