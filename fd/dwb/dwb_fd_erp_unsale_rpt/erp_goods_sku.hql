insert overwrite table dwd.dwd_fd_erp_goods_sku partition (pt = '${pt}')
select 	distinct
	  	tab2.goods_id,
	  	tab1.uniq_sku as goods_sku
from (
	select uniq_sku
	from ods_fd_ecshop.ods_fd_fd_sku_backups
)tab1
INNER JOIN (
	select
		external_goods_id as goods_id,
		uniq_sku
	from ods_fd_ecshop.ods_fd_ecs_goods
	where external_cat_id != 3002
	group by external_goods_id,uniq_sku
) tab2 on tab1.uniq_sku = tab2.uniq_sku
;