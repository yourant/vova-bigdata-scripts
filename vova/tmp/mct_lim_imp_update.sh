#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
with tmp_vova_cube_imp_cnt as 
(
select 
a.pt,
a.page,
a.hou,
c.rank,
b.first_cat_id,
c.mct_id,
count(*) as h_imp
from (
	select 
	case when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'traff_most_popular'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'traff_product_detail'
     when page_code ='search_result' and list_type in ('/search_result','/search_result_recommend') then 'traff_search_result'
     else '' end page,
	pt, 
	date_format(collector_ts,'H') as hou, 
	virtual_goods_id
	from 
	dwd.dwd_vova_log_goods_impression
	where datasource='vova' and platform='mob' and pt='${cur_date}'
) as a left join dim.dim_vova_goods b on a.virtual_goods_id = b.virtual_goods_id
	left join ads.ads_vova_mct_rank c on c.first_cat_id=b.first_cat_id and c.mct_id=b.mct_id
	where a.page <> '' and c.pt='${cur_date}'
group by a.pt,a.page,a.hou,c.rank,b.first_cat_id,c.mct_id
),

-- 计算到该时间段当日该等级商家累计在该页面该品类获得的商品曝光量和到该时间段该品类下的商品总曝光量
tmp_vova_imp_all_cnt as
(
select 
	distinct
	pt,
	page,
	hou,
	rank,
	first_cat_id,
	sum(h_imp) over(partition by page,pt,first_cat_id,rank order by hou) as rank_all_imp, 
	sum(h_imp) over(partition by page,pt,first_cat_id order by hou) as all_imp 
	from tmp_vova_cube_imp_cnt
),

-- 达到流量上限的商家个数 
tmp_vova_max_mct_cnt as
(
select 
	pt,
	page,
	date_format(cur_date,'H') as hou,
	rank,
	first_cat_id,
	count(distinct mct_id) as max_mct_cnt 
	from mlb.mlb_vova_mct_page_prob_d 
	where pt='${cur_date}' and max_flag=1 and page in ('traff_most_popular','traff_product_detail','traff_search_result') 
	group by pt,page,date_format(cur_date,'H'),rank,first_cat_id
),

--总商家个数
tmp_vova_mct_all_cnt as
(
select 
	pt,
	first_cat_id,
	rank,
	count(distinct mct_id) as all_mct_cnt 
	from ads.ads_vova_mct_rank 
	where pt='${cur_date}' 
	group by pt,first_cat_id,rank
),

--达到流量上限的商家获得的商品总曝光量
tmp_vova_max_all_imp as
(
select 
	distinct
	pt,
	page,
	hou,
	first_cat_id,
	sum(h_imp) over(partition by page,pt,first_cat_id order by hou) as max_imp
	from
	(select a.pt,a.page,date_format(a.cur_date,'H') as hou,a.rank,a.first_cat_id,a.mct_id,b.h_imp from mlb.mlb_vova_mct_page_prob_d a
		left join tmp_vova_cube_imp_cnt b on a.pt=b.pt and date_format(a.cur_date,'H')=b.hou and a.page=b.page and a.first_cat_id=b.first_cat_id and a.mct_id=b.mct_id and a.rank=b.rank
		where a.pt='${cur_date}' and a.max_flag=1
	) T
)

-- 统计商家分级限流情况
insert overwrite table tmp.tmp_vova_mct_lim_imp partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
	t1.pt,     --日期
	t1.page,   --页面
	t1.hou,    --小时
	t1.rank,   --商家等级
	g.first_cat_name,  --一级品类名称
	nvl(t2.max_mct_cnt,0) as max_mct_cnt,    --达到流量上限的商家个数
	nvl(t3.all_mct_cnt,0) as all_mct_cnt,    --总商家个数
	nvl(t4.max_imp,0) as max_imp,   	   --达到流量上限的商家获得的商品总曝光量
	t1.rank_all_imp,   --该等级商家总曝光量
	t1.all_imp         --商品总曝光量
	from tmp_vova_imp_all_cnt t1 
	left join tmp_vova_max_mct_cnt t2 on t1.pt=t2.pt and t1.page=t2.page and t1.hou=t2.hou and t1.rank=t2.rank and t1.first_cat_id=t2.first_cat_id
	left join tmp_vova_mct_all_cnt t3 on t1.pt=t3.pt and t1.rank=t3.rank and t1.first_cat_id=t3.first_cat_id
	left join tmp_vova_max_all_imp t4 on t1.pt=t4.pt and t1.page=t4.page and t1.hou=t4.hou and t1.first_cat_id=t4.first_cat_id
	left join (select distinct first_cat_id,first_cat_name from dim.dim_vova_category) g on t1.first_cat_id=g.first_cat_id;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=tmp_vova_mct_lim_imp" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi