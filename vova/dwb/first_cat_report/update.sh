#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

sql="
insert overwrite table dwb.dwb_vova_first_cat_report PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
to_date('${cur_date}') as event_date,
nvl(t2.datasource,'all') datasource,
nvl(t2.country,'all') country,
nvl(t2.is_activate,'all') is_activate,
'all' is_fbv,
nvl(t2.main_channel,'all') main_channel,
nvl(t2.first_cat_name,'all') first_cat_name,
sum(t2.expres) as expres,
sum(t2.clks) as clks,
count(distinct t2.pdbuy_device_id) as cart_uv,
count(distinct t2.cart_success_device_id) as cart_success_uv,
count(distinct t2.scr_device_id) as pd_uv
from
(
select
nvl(t1.datasource,'NA') datasource,
nvl(t1.country,'NA') country,
if(date(dev.activate_time) = '${cur_date}', 'Y', 'N')  as is_activate,
case when dev.main_channel is not null then dev.main_channel else 'NA' end main_channel,
nvl(g.first_cat_name,'NA') first_cat_name,
case when t1.element_name='pdAddToCartClick'  and t1.event_name ='common_click' then t1.device_id end pdbuy_device_id,
case when t1.element_name='pdAddToCartSuccess' and t1.event_name ='common_click' then t1.device_id end cart_success_device_id,
case when t1.event_name ='screen_view' then t1.device_id end scr_device_id,
t1.clks,
t1.expres
from
(
select datasource,event_name,virtual_goods_id,geo_country as country,device_id,dvce_created_tstamp,null element_name,1 clks,0 expres from dwd.dwd_vova_log_goods_click  where pt = '${cur_date}'
union all
select datasource,event_name,cast(element_id as bigint) virtual_goods_id,geo_country as country,device_id,dvce_created_tstamp,element_name ,0 clks,0 expres from dwd.dwd_vova_log_common_click where pt='${cur_date}' and element_name in('pdAddToCartSuccess','pdAddToCartClick') and page_code='product_detail' and platform ='mob' and device_id is not null
union all
select datasource,event_name,virtual_goods_id,geo_country as country,device_id,dvce_created_tstamp,null element_name ,0 clks,1 expres  from dwd.dwd_vova_log_goods_impression  where pt = '${cur_date}'
union all
select datasource,event_name,virtual_goods_id,geo_country as country,device_id,dvce_created_tstamp,null element_name ,0 clks,0 expres from dwd.dwd_vova_log_screen_view where pt = '${cur_date}' and page_code = 'product_detail' and platform ='mob' and view_type='show' and device_id is not null
) t1
left join dim.dim_vova_devices dev on dev.device_id = t1.device_id and dev.datasource = t1.datasource
left join dim.dim_vova_goods g on g.virtual_goods_id = t1.virtual_goods_id
) t2
group by
t2.datasource,
t2.country,
t2.is_activate,
t2.main_channel,
t2.first_cat_name
with cube
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql  --conf "spark.app.name=dwb_vova_first_cat_report_zhangyin" --conf "spark.dynamicAllocation.maxExecutors=150"  -e "$sql"
#hive -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi