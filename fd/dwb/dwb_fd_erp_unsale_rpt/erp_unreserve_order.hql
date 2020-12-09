insert overwrite table dwd.dwd_fd_erp_unreserve_order partition (pt = '${pt}')
SELECT
     /*+ REPARTITION(1) */
	eg.external_goods_id as goods_id,
	eg.uniq_sku as goods_sku,
	SUM(ins.demand_quantity) as goods_number
FROM (
	select
		product_id,
		available_to_reserved,
		demand_quantity
	from ods_fd_romeo.ods_fd_inventory_summary
	where STATUS_ID= 'INV_STTS_AVAILABLE' and facility_id = '383497303'
	group by product_id,available_to_reserved,demand_quantity

) ins
INNER JOIN (
	select
		external_goods_id,
		uniq_sku,
		product_id
	from ods_fd_ecshop.ods_fd_ecs_goods
	group by external_goods_id,uniq_sku,product_id

) eg ON ins.product_id = eg.product_id
GROUP BY eg.external_goods_id,eg.uniq_sku;