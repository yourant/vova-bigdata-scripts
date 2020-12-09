insert overwrite table dwb.dwb_fd_erp_unsale_detail  partition (pt = '${pt}')
select
     /*+ REPARTITION(1) */
	case
		when tab1.can_sale_days > tab1.back_days and tab1.can_sale_days <= 90 then 's1'
		when tab1.14d_avg_sale > 0 and tab1.14d_avg_sale < 5 and tab1.can_sale_days > 90 and tab1.can_sale_days <= 180 then 's2'
		when tab1.14d_avg_sale >= 5 and tab1.can_sale_days > 90 and tab1.can_sale_days <= 360 then 's2'
		when tab1.14d_avg_sale = 0 then 's3'
		when tab1.14d_avg_sale > 0 and tab1.14d_avg_sale < 5 and tab1.can_sale_days > 180 then 's3'
		when tab1.14d_avg_sale >= 5 and tab1.can_sale_days > 360 then 's3'
		when tab1.can_sale_days >= 0 and tab1.can_sale_days <= tab1.back_days then 's0'
	else 's0' end as unsale_level,
	tab1.goods_id,
	tab1.goods_sku,
	tab1.stock_days,
	tab1.14d_avg_sale,
	tab1.goods_number,
	tab1.reserve_num,
	tab1.goods_number_month,
	tab1.can_sale_days,
	tab1.back_days,
	case
		when tab1.14d_avg_sale = 0 then tab1.reserve_num
		when tab1.14d_avg_sale > 0 then tab1.14d_avg_sale * (tab1.can_sale_days - tab1.back_days)
	else 0 end as unsale_goods_num
from (
	select
		eg.goods_id,
		eg.goods_sku,
		nvl(egs.stock_days,0) as stock_days,
		nvl(eas.14d_avg_sale,0) as 14d_avg_sale,
		nvl(euo.goods_number,0) as goods_number,
		nvl(erg.reserve_num,0) as reserve_num,
		nvl(egsm.goods_number_month,0) as goods_number_month,
        CEILING(nvl((if((nvl(erg.reserve_num,0) - nvl(euo.goods_number,0)) > 0,nvl(erg.reserve_num,0) - nvl(euo.goods_number,0),0)) / eas.14d_avg_sale,0)) as can_sale_days,
		if(nvl(egs.stock_days,0)> 30,nvl(egs.stock_days,0),30) as back_days
	from (select goods_id,goods_sku from dwd.dwd_fd_erp_goods_sku where pt = '${pt}') eg
	LEFT JOIN (select goods_id,stock_days from dwd.dwd_fd_erp_goods_stock where pt = '${pt}') egs on egs.goods_id = eg.goods_id
	LEFT JOIN (select goods_id,goods_sku,14d_avg_sale from dwd.dwd_fd_erp_14d_avg_sale where pt = '${pt}') eas on eas.goods_id = eg.goods_id and eas.goods_sku = eg.goods_sku
	LEFT JOIN (select goods_id,goods_sku,goods_number from dwd.dwd_fd_erp_unreserve_order where pt = '${pt}') euo on euo.goods_id = eg.goods_id and euo.goods_sku = eg.goods_sku
	LEFT JOIN (select goods_id,goods_sku,reserve_num from dwd.dwd_fd_erp_reserve_goods where pt = '${pt}') erg on erg.goods_id = eg.goods_id and erg.goods_sku = eg.goods_sku
	LEFT JOIN (select goods_id,goods_sku,goods_number_month from dwd.dwd_fd_erp_goods_sale_monthly where pt = '${pt}') egsm on egsm.goods_id = eg.goods_id and egsm.goods_sku = eg.goods_sku
) tab1;