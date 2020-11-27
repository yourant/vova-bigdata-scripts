insert overwrite table dwb.dwb_fd_erp_unsale_rpt  partition (pt = '${pt}')
select
	tab1.unsale_level,
	tab1.unsale_rate as unsale_rate,
	tab1.unsale_goods_num as unsale_goods_num,
	tab1.goods_number_total as goods_number_total,
	if(tab1.unsale_level !='s0',((tab1.unsale_goods_num - tab2.unsale_goods_num) / tab2.unsale_goods_num),0.0) as ws_goods_number_rate
from (
	select
		nvl(t1.unsale_level,'all') as unsale_level,
		(sum(t1.unsale_goods_num) / sum(t1.goods_number_month)) as unsale_rate,
		sum(t1.unsale_goods_num) as unsale_goods_num,
		sum(t1.goods_number_month) as goods_number_total
	from(
	select
		unsale_level,
		if(unsale_level = 's0',0,cast(unsale_goods_num as bigint)) as unsale_goods_num,
		cast(goods_number_month as bigint) as goods_number_month
	from dwb.dwb_fd_erp_unsale_detail
	where pt = '${pt}'
	) t1 group by t1.unsale_level with cube

) tab1
left join(
	select
		nvl(t1.unsale_level,'all') as unsale_level,
		(sum(t1.unsale_goods_num) / sum(t1.goods_number_month)) as unsale_rate,
		sum(t1.unsale_goods_num) as unsale_goods_num,
		sum(t1.goods_number_month) as goods_number_total
	from(
	select
		unsale_level,
		if(unsale_level = 's0',0,cast(unsale_goods_num as bigint)) as unsale_goods_num,
		cast(goods_number_month as bigint) as goods_number_month
	from dwb.dwb_fd_erp_unsale_detail
	where pt = date_sub('${pt}',1)
	) t1 group by t1.unsale_level with cube

)tab2 on tab1.unsale_level = tab2.unsale_level;