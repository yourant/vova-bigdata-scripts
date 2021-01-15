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
	else 0 end as unsale_goods_num,
	if(stock_up_id is null,false,true) as is_spring_stock
from (
	select
		t0.goods_id,
		t0.goods_sku,
		nvl(t0.stock_days,0) as stock_days,
		nvl(t0.14d_avg_sale,0) as 14d_avg_sale,
		nvl(t0.goods_number,0) as goods_number,
		nvl(t0.reserve_num,0) as reserve_num,
		nvl(t0.goods_number_month,0) as goods_number_month,
        CEILING(nvl((if((nvl(t0.reserve_num,0) - nvl(t0.goods_number,0)) > 0,nvl(t0.reserve_num,0) - nvl(t0.goods_number,0),0)) / t0.14d_avg_sale,0)) as can_sale_days,
		if(nvl(t0.stock_days,0)> 30,nvl(t0.stock_days,0),30) as back_days,
		t1.stock_up_id
	from (
	dwd.dwd_fd_erp_unsale_goods_info t0
    LEFT JOIN  dwd.dwd_fd_spring_festival_stock_up_info  t1 on t1.goods_id = t0.goods_id
    and t0.pt='${pt}'
) )tab1;