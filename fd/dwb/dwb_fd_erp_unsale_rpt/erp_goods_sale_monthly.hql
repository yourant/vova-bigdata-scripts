insert overwrite table dwd.dwd_fd_erp_goods_sale_monthly partition (pt = '${pt}')
select
 /*+ REPARTITION(1) */
	eg.external_goods_id as goods_id,
	eg.uniq_sku as goods_sku,
	sum(eog.goods_number) as goods_number_month
from (
	select goods_id,order_id,goods_number
	from ods_fd_ecshop.ods_fd_ecs_order_goods
	group by goods_id,order_id,goods_number
)eog
INNER JOIN (
	select order_id
	from ods_fd_ecshop.ods_fd_ecs_order_info
	where pay_status = 2
	and order_type_id = 'SALE'
	and to_date(to_utc_timestamp(order_time, 'PRC')) >= trunc('${pt}','MM')
	and to_date(to_utc_timestamp(order_time, 'PRC')) <= '${pt}'
	group by order_id
)eoi on eoi.order_id = eog.order_id
INNER JOIN (
	select external_goods_id,uniq_sku,goods_id
	from ods_fd_ecshop.ods_fd_ecs_goods
)eg on eog.goods_id = eg.goods_id
group by eg.external_goods_id,eg.uniq_sku;