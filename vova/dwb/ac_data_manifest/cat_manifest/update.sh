#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: $cur_date"

job_name="dwb_vova_second_cat_manifest_req4947_chenkai"

#
sql="
set spark.sql.adaptive.enabled=true;
insert overwrite table dwb.dwb_vova_second_cat_manifest  PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
re.datasource,
re.region_code,
if(re.second_cat_id = 'all', 'all', first_cat_name) first_cat_name,
if(re.second_cat_id = 'all', 'all', second_cat_name) second_cat_name,
nvl(if(re.second_cat_id = 'all', search_second_cat_uv, search_first_cat_uv), 0) search_first_cat_uv,
nvl(search_second_cat_uv, 0),
nvl(add_cart_uv, 0),
nvl(pd_uv, 0),
nvl(pay_uv, 0)
from 
(select 
result1.datasource datasource,
result1.region_code region_code,
if(result1.second_cat_id = 'all', 'all', result2.first_cat_id) first_cat_id,
result1.second_cat_id second_cat_id,
if(result1.second_cat_id = 'all', result1.search_second_cat_uv, result2.search_first_cat_uv) search_first_cat_uv,
result1.search_second_cat_uv search_second_cat_uv,
result1.add_cart_uv add_cart_uv,
result1.pd_uv pd_uv,
tmp_pay_uv.pay_uv pay_uv
from 
 (select
   nvl(t2.datasource,'all') datasource, -- 数据源
   nvl(t2.region_code,'all') region_code, -- 国家
   nvl(t2.second_cat_id,'all') second_cat_id, -- 二级品类
   nvl(count(distinct(search_second_cat_device_id)),0) search_second_cat_uv, -- search页数据二级品类点击UV
   nvl(count(distinct(add_cart_device_id)),0) add_cart_uv, -- 加购按钮UV
   nvl(count(distinct(pd_device_id)),0) pd_uv -- 商详页UV
  from (
     select
      nvl(t1.datasource,'NA') datasource, -- 数据源
      nvl(t1.region_code,'NA') region_code, -- 国家
      nvl(dg.second_cat_id,'NA') second_cat_id, -- 二级品类
   
      case when t1.page_code = 'search_result' and t1.event_name ='goods_click' then t1.device_id end search_second_cat_device_id, -- search页数据二级品类点击UV
      case when t1.element_name='pdAddToCartClick'  and t1.event_name ='common_click' then t1.device_id end add_cart_device_id, -- 加购按钮UV
      CASE when t1.page_code='product_detail' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end pd_device_id -- 商详页UV

     from 
     ( -- platform os_type,page_code,device_id,NULL element_name 
      select datasource,event_name,platform,os_type,page_code,view_type,virtual_goods_id,
          geo_country as region_code,device_id,dvce_created_tstamp,'goods_click' element_name,1 clks,0 expres,pt
       from dwd.dwd_vova_log_goods_click
       where pt = '${cur_date}' and page_code = 'search_result' and dp='airyclub'
      union all
      select datasource,event_name,platform,os_type,page_code,view_type,cast(element_id as bigint) virtual_goods_id,
          geo_country as region_code,device_id,dvce_created_tstamp,element_name ,0 clks,0 expres,pt
       from dwd.dwd_vova_log_common_click
       where pt = '${cur_date}'
       and element_name ='pdAddToCartClick' and page_code='product_detail' and device_id is not null
       and dp='airyclub'
      union all 
      select datasource,event_name,platform,os_type,page_code,view_type,virtual_goods_id,
          geo_country as region_code,device_id,dvce_created_tstamp,null element_name ,0 clks,0 expres,pt
       from dwd.dwd_vova_log_screen_view
       where pt = '${cur_date}'
       and page_code = 'product_detail' and view_type='show' and device_id is not null and dp='airyclub'
     ) t1 
    left join 
      dim.dim_vova_goods dg
    on t1.virtual_goods_id = dg.virtual_goods_id
    where dg.virtual_goods_id is not null and dg.second_cat_id is not null
   ) t2
   group by cube(t2.datasource, t2.region_code, t2.second_cat_id)
   HAVING region_code in ('all','GB','FR','DE','IT','ES','NL','PT','US','CS','PL','BE','MX','SI','RU','JP','BR','TW','NA','AU')
) result1 

left join
(
 select 
  nvl(dog.datasource,'all') datasource, -- 数据源
  nvl(dog.region_code, 'all') region_code, -- 国家
  nvl(dg.second_cat_id, 'all') second_cat_id, -- 二级品类
  nvl(count(distinct(dog.device_id)),0) pay_uv -- 所有商品已付款UV
 from dim.dim_vova_order_goods dog
 left join
   dim.dim_vova_goods dg
 on dog.goods_id = dg.goods_id
 where to_date(dog.order_time) = '${cur_date}' and dog.sku_pay_status = 2 and dg.goods_id is not null and dg.second_cat_id is not null and datasource = 'airyclub'
 group by cube(dog.datasource, dog.region_code, dg.second_cat_id)
 HAVING region_code in ('all','GB','FR','DE','IT','ES','NL','PT','US','CS','PL','BE','MX','SI','RU','JP','BR','TW','NA','AU')
) tmp_pay_uv
on result1.region_code = tmp_pay_uv.region_code and result1.second_cat_id = tmp_pay_uv.second_cat_id and result1.datasource = tmp_pay_uv.datasource

left join 
(
 select 
 t_re.datasource,
 region_code,
 t_re.first_cat_id,
 search_first_cat_uv,
 second_cat_id
 from 
 (select 
  nvl(t2.datasource,'all') datasource, -- 数据源
  nvl(t2.region_code,'all') region_code, -- 国家
  nvl(t2.first_cat_id,'all') first_cat_id, -- 一级品类
  nvl(count(distinct(search_first_cat_device_id)),0) search_first_cat_uv -- search页数据一级品类点击UV
  from 
  (select 
   nvl(t1.datasource,'NA') datasource, -- 数据源
   nvl(t1.region_code,'NA') region_code, -- 国家
   nvl(dg.first_cat_id,'NA') first_cat_id, -- 一级品类
   case when t1.page_code = 'search_result' and t1.event_name ='goods_click' then t1.device_id end search_first_cat_device_id -- search页数据一级品类点击UV  
   from 
     (select datasource,event_name,platform,os_type,page_code,view_type,virtual_goods_id,
          geo_country as region_code,device_id,dvce_created_tstamp,'goods_click' element_name,1 clks,0 expres,pt
       from dwd.dwd_vova_log_goods_click
       where pt = '${cur_date}' and page_code = 'search_result' and dp='airyclub') t1
     left join 
      dim.dim_vova_goods dg
     on t1.virtual_goods_id = dg.virtual_goods_id
     where dg.virtual_goods_id is not null and dg.first_cat_id is not null
  ) t2
  group by cube(region_code, first_cat_id, datasource)
  HAVING first_cat_id !='all' and region_code in ('all','GB','FR','DE','IT','ES','NL','PT','US','CS','PL','BE','MX','SI','RU','JP','BR','TW','NA','AU')
 ) t_re
  left join
  (select distinct first_cat_id, second_cat_id from dim.dim_vova_goods) dg
  on dg.first_cat_id = t_re.first_cat_id
  where dg.second_cat_id is not null
) result2
on result1.region_code = result2.region_code and result1.second_cat_id = result2.second_cat_id and result1.datasource = result2.datasource) re 
left join 
(select distinct first_cat_id, first_cat_name, second_cat_id, second_cat_name from dim.dim_vova_goods) dg
on re.second_cat_id = dg.second_cat_id
where datasource = 'airyclub'
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`