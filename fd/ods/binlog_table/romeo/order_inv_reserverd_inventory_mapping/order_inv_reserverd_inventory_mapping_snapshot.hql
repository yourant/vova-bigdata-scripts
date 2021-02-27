INSERT overwrite table ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping
select /*+ REPARTITION(1) */ id,order_inv_reserved_detail_id,inventory_item_id,quantity,created_stamp,last_updated_stamp
from ods_fd_romeo.ods_fd_order_inv_reserverd_inventory_mapping_arc
where pt = '${pt}';
