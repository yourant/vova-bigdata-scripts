#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

echo "cur_date: $cur_date"

job_name="dwb_vova_goods_manifest_req5264_chenkai_${cur_date}"

#
sql="
set spark.sql.adaptive.enabled=true;

with tmp_rpt_goods_manifest as(
select /*+ REPARTITION(5) */
result1.datasource,
result1.region_code,
result1.source_platform,
result1.mct_id,
dg.second_cat_name second_cat_name,
result1.goods_id,

result1.day5_pd_uv,
result1.day5_add_cart_uv,
result1.day5_expres, -- 曝光
result1.day5_clks, -- 点击
result1.day3_pd_uv,
result1.day3_add_cart_uv,
result1.day3_expres,
result1.day3_clks,
result1.day1_pd_uv,
result1.day1_add_cart_uv,
result1.day1_expres,
result1.day1_clks,

tmp_dog.day5_order_uv,
tmp_dog.day5_goods_number,
tmp_dog.day5_gmv,
tmp_dog.day5_pay_uv,
tmp_dog.day3_order_uv,
tmp_dog.day3_goods_number,
tmp_dog.day3_gmv,
tmp_dog.day3_pay_uv,
tmp_dog.day1_order_uv,
tmp_dog.day1_goods_number,
tmp_dog.day1_gmv,
tmp_dog.day1_pay_uv
from
(select
 nvl(datasource, 'all') datasource,
 nvl(region_code,'all') region_code, -- 国家
 nvl(source_platform, 'all') source_platform,
 nvl(mct_id, 'all') mct_id,
 nvl(goods_id,'all') goods_id,
 count(distinct t2.day5_pd_device_id) as day5_pd_uv, -- 近5天商详页UV
 count(distinct t2.day5_add_cart_device_id) as day5_add_cart_uv, -- 近5天加购UV
 sum(day5_expres) day5_expres, -- 近5天impression
 sum(day5_clks) day5_clks, -- 近5天click

 count(distinct t2.day3_pd_device_id) as day3_pd_uv, --近3天商详页UV
 count(distinct t2.day3_add_cart_device_id ) as day3_add_cart_uv, -- 近3天加购UV
 sum(day3_expres) day3_expres, --近3天impression
 sum(day3_clks) day3_clks, -- 近3天click

 count(distinct t2.day1_pd_device_id ) as day1_pd_uv, --近1天商详页UV
 count(distinct t2.day1_add_cart_device_id) as day1_add_cart_uv, -- 近1天加购UV
 sum(day1_expres) day1_expres, -- 近1天impression
 sum(day1_clks) day1_clks -- 近1天click
from
 (select
   nvl(t1.datasource,'NA') datasource, -- datasource
   nvl(t1.region_code,'NA') region_code, -- 国家
   case when t1.os_type='android' then 'android'
        when t1.os_type='ios' then 'ios'
        else 'web' end source_platform, -- 平台
   dg.mct_id mct_id, -- 店铺id
   dg.goods_id goods_id, -- 商品id

   case when t1.element_name='pdAddToCartClick' and t1.event_name ='common_click' then t1.device_id end day5_add_cart_device_id, -- 加购UV
   CASE when t1.page_code='product_detail' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end day5_pd_device_id, -- 商详页UV
   t1.expres day5_expres, -- 曝光
   t1.clks day5_clks, -- 点击

   case when pt > date_sub('${cur_date}', 3) and t1.element_name='pdAddToCartClick' and t1.event_name ='common_click' then t1.device_id end day3_add_cart_device_id, -- 加购UV
   CASE when pt > date_sub('${cur_date}', 3) and t1.page_code='product_detail' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end day3_pd_device_id, -- 商详页UV
   CASE when pt > date_sub('${cur_date}', 3) then t1.expres
        else 0 end day3_expres, -- 曝光
   CASE when pt > date_sub('${cur_date}', 3) then t1.clks
        else 0 end day3_clks, -- 点击

   case when pt = '${cur_date}' and t1.element_name='pdAddToCartClick'  and t1.event_name ='common_click' then t1.device_id end day1_add_cart_device_id, -- 加购UV
   CASE when t1.page_code='product_detail' and t1.view_type='show' and t1.event_name ='screen_view' THEN t1.device_id end day1_pd_device_id, -- 商详页UV
   CASE when pt = '${cur_date}' then t1.expres
        else 0 end day1_expres, -- 曝光
   CASE when pt = '${cur_date}' then t1.clks
        else 0 end day1_clks -- 点击

  from
  ( -- platform os_type,page_code,device_id,NULL element_name
   select datasource,event_name,platform,os_type,page_code,view_type,virtual_goods_id,
       geo_country as region_code,device_id,dvce_created_tstamp,null element_name,1 clks,0 expres,pt
    from dwd.dwd_vova_log_goods_click
    where pt <= '${cur_date}' and pt > date_sub('${cur_date}', 5) and device_id is not null and dp='airyclub'
   union all
   select datasource,event_name,platform,os_type,page_code,view_type,cast(element_id as bigint) virtual_goods_id,
       geo_country as region_code,device_id,dvce_created_tstamp,element_name ,0 clks,0 expres,pt
    from dwd.dwd_vova_log_common_click
    where pt <= '${cur_date}' and pt > date_sub('${cur_date}', 5)
    and element_name ='pdAddToCartClick' and page_code='product_detail' and device_id is not null and dp='airyclub'
   union all
   select datasource,event_name,platform,os_type,page_code,view_type,virtual_goods_id,
       geo_country as region_code,device_id,dvce_created_tstamp,null element_name ,0 clks,1 expres,pt
    from dwd.dwd_vova_log_goods_impression
    where pt <= '${cur_date}' and pt > date_sub('${cur_date}', 5) and device_id is not null and dp='airyclub'
   union all
   select datasource,event_name,platform,os_type,page_code,view_type,virtual_goods_id,
       geo_country as region_code,device_id,dvce_created_tstamp,'screen_view' element_name ,0 clks,0 expres,pt
    from dwd.dwd_vova_log_screen_view
    where pt <= '${cur_date}' and pt > date_sub('${cur_date}', 5)
    and page_code = 'product_detail' and view_type='show' and device_id is not null and dp='airyclub'
  ) t1
  left join
  dim.dim_vova_goods dg
  on t1.virtual_goods_id = dg.virtual_goods_id
  where dg.virtual_goods_id is not null
 ) t2 where goods_id is not null
 group by cube(
 datasource,
 region_code,
 source_platform,
 mct_id,
 goods_id)
) result1
left join
(
 select
 nvl(datasource, 'all') datasource,
 nvl(region_code,'all') region_code,
 nvl(source_platform, 'all') source_platform,
 nvl(mct_id, 'all') mct_id,
 nvl(goods_id,'all') goods_id,

 nvl(count(distinct(day5_order_uv_device_id)),0) day5_order_uv, -- 下单UV
 nvl(sum(day5_goods_number),0) day5_goods_number, -- 销量
 nvl(sum(day5_gmv),0) day5_gmv, -- gmv
 nvl(count(distinct(day5_pay_uv_device_id)),0) day5_pay_uv, -- 支付UV

 nvl(count(distinct(day3_order_uv_device_id)),0) day3_order_uv, -- 下单UV
 nvl(sum(day3_goods_number),0) day3_goods_number, -- 销量
 nvl(sum(day3_gmv),0) day3_gmv, -- gmv
 nvl(count(distinct(day3_pay_uv_device_id)),0) day3_pay_uv, -- 支付UV

 nvl(count(distinct(day1_order_uv_device_id)),0) day1_order_uv, -- 下单UV
 nvl(sum(day1_goods_number),0) day1_goods_number, -- 销量
 nvl(sum(day1_gmv),0) day1_gmv, -- gmv
 nvl(count(distinct(day3_pay_uv_device_id)),0) day1_pay_uv -- 支付UV
 from
 (select *,
   case when platform='android' then 'android'
        when platform='ios' then 'ios'
        else 'web' end source_platform, -- 平台

   case when to_date(order_time)> date_sub('${cur_date}', 5) then device_id end day5_order_uv_device_id,
   case when to_date(order_time)> date_sub('${cur_date}', 5) and pay_status >= 1 then goods_number end day5_goods_number,
   case when to_date(order_time)> date_sub('${cur_date}', 5) and pay_status >= 1 then goods_number * shop_price + shipping_fee end day5_gmv,
   case when to_date(order_time)> date_sub('${cur_date}', 5) and pay_status >= 1 then device_id end day5_pay_uv_device_id,

   case when to_date(order_time)> date_sub('${cur_date}', 3) then  device_id end day3_order_uv_device_id,
   case when to_date(order_time)> date_sub('${cur_date}', 3) and pay_status >= 1 then goods_number end day3_goods_number,
   case when to_date(order_time)> date_sub('${cur_date}', 3) and pay_status >= 1 then goods_number * shop_price + shipping_fee end day3_gmv,
   case when to_date(order_time)> date_sub('${cur_date}', 3) and pay_status >= 1 then device_id end day3_pay_uv_device_id,

   case when to_date(order_time)='${cur_date}' then device_id end day1_order_uv_device_id,
   case when to_date(order_time)='${cur_date}' and pay_status >= 1 then goods_number end day1_goods_number,
   case when to_date(order_time)='${cur_date}' and pay_status >= 1 then goods_number * shop_price + shipping_fee end day1_gmv,
   case when to_date(order_time)='${cur_date}' and pay_status >= 1 then device_id end day1_pay_uv_device_id
  from dim.dim_vova_order_goods
  where to_date(order_time)<='${cur_date}' and to_date(order_time)> date_sub('${cur_date}', 5)
 ) dog
 group by cube(
 datasource,
 region_code,
 source_platform,
 mct_id,
 goods_id
 )
) tmp_dog
on result1.datasource = tmp_dog.datasource
and result1.region_code = tmp_dog.region_code
and result1.source_platform = tmp_dog.source_platform
and result1.goods_id = tmp_dog.goods_id
and result1.mct_id = tmp_dog.mct_id

left join
dim.dim_vova_goods dg
on dg.goods_id = result1.goods_id
where tmp_dog.datasource is not null and tmp_dog.region_code is not null
 and tmp_dog.source_platform is not null and tmp_dog.goods_id is not null and tmp_dog.mct_id is not null
 and dg.second_cat_name is not null
)

-- 结果输出
insert overwrite table dwb.dwb_vova_goods_manifest  PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
datasource datasource       ,
region_code region_code      ,
source_platform source_platform  ,
mct_id mct_id           ,
5 last_days        ,
goods_id goods_id         ,
second_cat_name second_cat_name,
day5_expres impression_pv    ,
day5_clks click_pv         ,
day5_add_cart_uv add_cart_uv      ,
day5_pd_uv pd_uv            ,
day5_order_uv order_uv         ,
day5_pay_uv pay_uv      ,
day5_goods_number goods_number,
day5_gmv gmv
from tmp_rpt_goods_manifest
where day5_goods_number > 0
and region_code in ('all','GB','FR','DE','IT','ES','NL','PT','ES','US','CS','PL','BE','MX','SI','RU','JP','BR','TW','NA','AU')
and datasource = 'airyclub' and goods_id != 'all' and second_cat_name != 'all' and mct_id != 'all'
union all
select
/*+ REPARTITION(1) */
datasource datasource       ,
region_code region_code      ,
source_platform source_platform  ,
mct_id mct_id           ,
3 last_days        ,
goods_id goods_id         ,
second_cat_name second_cat_name,
day3_expres impression_pv    ,
day3_clks click_pv         ,
day3_add_cart_uv add_cart_uv      ,
day3_pd_uv pd_uv            ,
day3_order_uv order_uv         ,
day3_pay_uv pay_uv      ,
day3_goods_number goods_number,
day3_gmv gmv
from tmp_rpt_goods_manifest
where day3_goods_number > 0
and region_code in ('all','GB','FR','DE','IT','ES','NL','PT','ES','US','CS','PL','BE','MX','SI','RU','JP','BR','TW','NA','AU')
and datasource = 'airyclub' and goods_id != 'all' and second_cat_name != 'all' and mct_id != 'all'
union all
select
/*+ REPARTITION(1) */
datasource datasource       ,
region_code region_code      ,
source_platform source_platform  ,
mct_id mct_id           ,
1 last_days        ,
goods_id goods_id         ,
second_cat_name second_cat_name,
day1_expres impression_pv    ,
day1_clks click_pv         ,
day1_add_cart_uv add_cart_uv      ,
day1_pd_uv pd_uv            ,
day1_order_uv order_uv         ,
day1_pay_uv pay_uv      ,
day1_goods_number goods_number,
day1_gmv gmv
from tmp_rpt_goods_manifest
where day1_goods_number > 0
and region_code in ('all','GB','FR','DE','IT','ES','NL','PT','ES','US','CS','PL','BE','MX','SI','RU','JP','BR','TW','NA','AU')
and datasource = 'airyclub' and goods_id != 'all' and second_cat_name != 'all' and mct_id != 'all'
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 12G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`