insert overwrite table dwd.dwd_fd_erp_goods_stock partition (pt = '${pt}')
select
	goods_id as goods_id,
	(2 * produce_days + change_provider_days + 2) as stock_days
from ods_fd_ecshop.ods_fd_fd_stock_goods_config;