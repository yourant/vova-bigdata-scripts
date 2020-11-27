set hive.support.quoted.identifiers=None;
INSERT overwrite table ods_fd_ecshop.ods_fd_fd_stock_ecs_order_sale_bak_detail
select id,bak_id,bak_order_date,external_goods_id,on_sale_time,7d_sale,14d_sale,28d_sale,uniq_sku
from ods_fd_ecshop.ods_fd_fd_stock_ecs_order_sale_bak_detail_arc
where pt = '${pt}';
