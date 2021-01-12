insert overwrite table dwb.dwb_fd_erp_14d_avg_sale partition (pt = '${pt}')
select
 /*+ REPARTITION(1) */
	external_goods_id as goods_id,
	uniq_sku as goods_sku,
	sum(14d_sale) as 14d_avg_sale
from ods_fd_ecshop.ods_fd_fd_stock_ecs_order_sale_bak_detail
group by external_goods_id,uniq_sku;