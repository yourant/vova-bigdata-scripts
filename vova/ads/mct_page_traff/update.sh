#!/bin/bash
#指定日期和引擎
stime=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  stime=`date -d "-1 hour" "+%Y-%m-%d %H:%M:%S"`
fi
echo "$stime"
#默认小时
pt=`date -d "$stime" +%Y-%m-%d`
echo "$pt"
hour=`date -d "$stime" +%H`
echo "$hour"

sql="
insert overwrite table ads.ads_vova_mct_page_traff_h PARTITION (pt = '${pt}',hour='$hour')
select
/*+ REPARTITION(1) */
if(vg.mct_id is not null,vg.mct_id,g.mct_id) mct_id,
g.first_cat_id,
nvl(page,'traff_all') page,
t.country,
sum(expre) expre_cnt_1h
from
(
select
virtual_goods_id,
case when page_code = 'homepage' and list_type='/popular' then 'traff_best_selling'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'traff_most_popular'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'traff_product_detail'
     when page_code ='search_result' and list_type in ('/search_result','/search_result_recommend') then 'traff_search_result'
     else 'traff_others' end page,
geo_country country,
1 expre
from dwd.dwd_vova_log_goods_impression_arc where pt='$pt' and  hour ='$hour'  and platform='mob' and datasource='vova' and geo_country is not null
union all
select
cast (element_id as bigint ) virtual_goods_id,
case when page_code = 'homepage' and list_type='/popular' then 'traff_best_selling'
     when page_code in ('homepage','product_list') and  list_type in ('/product_list_popular','/product_list') then 'traff_most_popular'
     when page_code ='product_detail' and list_type ='/detail_also_like' then 'traff_product_detail'
     when page_code ='search_result' and list_type in ('/search_result','/search_result_recommend') then 'traff_search_result'
     else 'traff_others' end page,
geo_country country,
1 expre
from dwd.dwd_vova_log_impressions_arc
where pt='$pt' and hour = '$hour' and platform='mob' and event_type='goods' and datasource='vova'  and geo_country is not null
) t
left join dim.dim_vova_virtual_six_mct_goods vg on vg.virtual_goods_id = t.virtual_goods_id
left join dim.dim_vova_goods g on t.virtual_goods_id = g.virtual_goods_id
where g.first_cat_id is not null and g.mct_id is not null
GROUP BY if(vg.mct_id is not null,vg.mct_id,g.mct_id),g.first_cat_id,t.page,t.country grouping sets ((if(vg.mct_id is not null,vg.mct_id,g.mct_id),g.first_cat_id,t.page,t.country),(if(vg.mct_id is not null,vg.mct_id,g.mct_id),g.first_cat_id,country));
"
spark-sql --conf "spark.app.name=ads_vova_mct_page_traff_h_zhangyin" --conf spark.dynamicAllocation.maxExecutors=100  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi