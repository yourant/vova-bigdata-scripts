#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
   cur_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table ads.ads_vova_not_brand_goods_tag_data partition (pt = '${cur_date}')
select distinct a.goods_id,1 as tag_id,8 as weight from dim.dim_vova_goods a
join (select distinct mct_id,rank from ads.ads_vova_mct_rank where pt='${cur_date}') b on a.mct_id=b.mct_id
where a.brand_id=0 and b.rank=5
and a.is_on_sale=1 and a.goods_id in (select distinct goods_id from
ads.ads_vova_goods_imp_detail
where pt='${cur_date}'
and page_code='homepage' and list_type='/popular')
UNION
select distinct a.goods_id,2 as tag_id,6 as weight from dim.dim_vova_goods a
join (select distinct mct_id,rank from ads.ads_vova_mct_rank where pt='${cur_date}') b on a.mct_id=b.mct_id
where a.brand_id=0 and b.rank=6
and a.is_on_sale=1 and a.goods_id in (select distinct goods_id from
ads.ads_vova_goods_imp_detail
where pt='${cur_date}'
and page_code='homepage' and list_type='/popular')
UNION
select
	goods_id,3 as tag_id,3 as weight
	from (
	select a.goods_id,count(*) as cnt,sum(case when a.rating=5 then 1 else 0 end) as good_cnt
	from dwd.dwd_vova_fact_comment a
	join dim.dim_vova_goods b on a.goods_id=b.goods_id
	where date(a.post_time) > date_sub('${cur_date}',30)
	and b.brand_id=0
	and b.is_on_sale=1
	group by a.goods_id
	) t where cnt>=3 and round(good_cnt/cnt,2) > 0.7 and goods_id in (select distinct goods_id from
ads.ads_vova_goods_imp_detail
where pt='${cur_date}'
and page_code='homepage' and list_type='/popular')

UNION
select
	goods_id,4 as tag_id,5 as weight
	from (
		select
			a.goods_id
		from dwd.dwd_vova_fact_pay a
		join dim.dim_vova_goods b on a.goods_id=b.goods_id
		where date(a.pay_time) > date_sub('${cur_date}',30)
		and b.brand_id=0
		and b.is_on_sale=1
		and a.goods_id in (select distinct goods_id from
							ads.ads_vova_goods_imp_detail
							where pt='${cur_date}'
							and page_code='homepage' and list_type='/popular')
		group by a.goods_id having count(*) >= 5
	) T
UNION
select
distinct fp.goods_id,5 as tag_id,4 as weight
from
dwd.dwd_vova_fact_pay fp
left join dwd.dwd_vova_fact_logistics fl  on fl.order_goods_id = fp.order_goods_id
join dim.dim_vova_goods b on fp.goods_id=b.goods_id
where fl.valid_tracking_date is not null and date(fl.valid_tracking_date) != '1970-01-01'
and date(fp.confirm_time)>date_sub(date(fl.valid_tracking_date),2)
and b.brand_id=0
and b.is_on_sale=1
and fp.goods_id  in (select distinct goods_id from
							ads.ads_vova_goods_imp_detail
							where pt='${cur_date}'
							and page_code='homepage' and list_type='/popular')
UNION
select
distinct goods_id,6 as tag_id,7 as weight
	from (
	select
	a.goods_id
	from
	ads.ads_vova_goods_imp_detail a
	join dim.dim_vova_goods b on a.goods_id=b.goods_id
	where a.pt > date_sub('${cur_date}',30)
	and b.brand_id=0
	and b.is_on_sale=1
	and a.goods_id in (select distinct goods_id from
							ads.ads_vova_goods_imp_detail
							where pt='${cur_date}'
							and page_code='homepage' and list_type='/popular')
	group by a.goods_id having sum(a.expre_cnt)>2000
	) T
UNION
select distinct a.goods_id,7 as tag_id,1 as weight
 from dim.dim_vova_goods a
join (select distinct mct_id,rank from ads.ads_vova_mct_rank where pt='${cur_date}') b on a.mct_id=b.mct_id
where a.brand_id=0
and a.is_on_sale=1
and a.goods_id in (select distinct goods_id from
							ads.ads_vova_goods_imp_detail
							where pt='${cur_date}'
							and page_code='homepage' and list_type='/popular')
and b.rank>=5 and date(a.add_time)>date_sub('${cur_date}',30)
UNION
select a.goods_id,8 as tag_id,2 as weight
from dwb.dwb_vova_red_packet_goods a
join dim.dim_vova_goods b on a.goods_id=b.goods_id
 where a.pt='${cur_date}' and b.brand_id=0
 and b.is_on_sale=1
 and a.goods_id in (select distinct goods_id from
							ads.ads_vova_goods_imp_detail
							where pt='${cur_date}'
							and page_code='homepage' and list_type='/popular')
"
spark-sql --conf "spark.app.name=tmp_vova_cat_value_goods" --conf "spark.dynamicAllocation.maxExecutors=300" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi