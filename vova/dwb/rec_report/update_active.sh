#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
pre_week=`date -d "7 day ago ${cur_date}" +%Y-%m-%d`
pre_date=`date -d "1 day ago ${cur_date}" +%Y-%m-%d`
###逻辑sql
sql="

INSERT OVERWRITE TABLE  tmp.fact_impressions_5883_page_uv
select /*+ REPARTITION(40) */ datasource,country,os_type,page_code,element_type,list_type,activate_time,device_id_expre from (
select
nvl(gi.datasource,'NA') datasource,
nvl(geo_country,'NA') country,
nvl(os_type,'NA') os_type,
nvl(page_code,'NA') page_code,
nvl(element_type,'NA') element_type,
nvl(list_type,'NA') list_type,
CASE WHEN datediff(gi.pt,d.activate_time)<=0 THEN 'new'
     WHEN datediff(gi.pt,d.activate_time)>=1 and datediff(gi.pt,d.activate_time)<2 THEN '2-3'
     WHEN datediff(gi.pt,d.activate_time)>=3 and datediff(gi.pt,d.activate_time)<6 THEN '4-7'
     WHEN datediff(gi.pt,d.activate_time)>=7 and datediff(gi.pt,d.activate_time)<29 THEN '8-30'
     else '30+' END activate_time,
gi.device_id device_id_expre
from dwd.dwd_vova_log_goods_impression gi
join dim.dim_vova_devices d on gi.device_id = d.device_id and gi.datasource=d.datasource
where pt='$cur_date'  and os_type in ('ios','android')
union all
select
nvl(gi.datasource,'NA') datasource,
nvl(geo_country,'NA') country,
nvl(os_type,'NA') os_type,
nvl(page_code,'NA') page_code,
nvl(element_type,'NA') element_type,
nvl(list_type,'NA') list_type,
CASE WHEN datediff(gi.pt,d.activate_time)<=0 THEN 'new'
     WHEN datediff(gi.pt,d.activate_time)>=1 and datediff(gi.pt,d.activate_time)<2 THEN '2-3'
     WHEN datediff(gi.pt,d.activate_time)>=3 and datediff(gi.pt,d.activate_time)<6 THEN '4-7'
     WHEN datediff(gi.pt,d.activate_time)>=7 and datediff(gi.pt,d.activate_time)<29 THEN '8-30'
     else '30+' END activate_time,
gi.device_id device_id_expre
from dwd.dwd_vova_log_impressions gi
join dim.dim_vova_devices d on gi.device_id = d.device_id and gi.datasource=d.datasource
where pt='$cur_date'  and os_type in ('ios','android')) t
;

INSERT OVERWRITE TABLE  tmp.vova_rec_report_tmp
select /*+ REPARTITION(4) */
nvl(datasource,'all') datasource,
nvl(if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(element_type,'all') element_type,
nvl(list_type,'all') list_type,
nvl(activate_time,'all') activate_time,
count(distinct device_id_expre)  as page_uv
from
(select datasource,
if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),
os_type,
page_code,
element_type,
list_type,
activate_time,
device_id_expre
from tmp.fact_impressions_5883_page_uv
datasource,
if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),
os_type,
page_code,
element_type,
list_type,
activate_time,
device_id_expre) t
group by
datasource,
if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),
os_type,
page_code,
element_type,
list_type,
activate_time
with cube
;

INSERT OVERWRITE TABLE  tmp.vova_rec_report
select
/*+ REPARTITION(4) */
t1.datasource,
t1.country,
t1.os_type,
t1.page_code,
t1.element_type,
t1.list_type,
t1.activate_time,
nvl(t1.expres,0) expres,
nvl(t1.clks,0) clks,
nvl(t1.clk_uv,0) clk_uv,
nvl(t1.expre_uv,0) expre_uv,
nvl(t2.cart_uv,0) cart_uv,
nvl(t3.order_number,0) order_number,
nvl(t4.payed_number,0) payed_number,
nvl(t4.payed_uv,0) payed_uv,
nvl(t4.gmv,0) gmv,
nvl(t4.payed_gds_num,0) payed_gds_num,
nvl(t5.total_gmv,0) total_gmv,
nvl(t6.page_uv,0) page_uv
from
(
select
nvl(datasource,'all') datasource,
nvl(if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(element_type,'all') element_type,
nvl(list_type,'all') list_type,
nvl(activate_time,'all') activate_time,
sum(expres) as expres,
sum(clks) as clks,
count(distinct device_id_clk) as clk_uv,
count(distinct device_id_expre) as expre_uv
from dwd.dwd_vova_rec_report_clk_expre  where pt = '$cur_date'
group by
datasource,
if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),
os_type,
page_code,element_type,
list_type,
activate_time
with cube
) t1
left join
(
select
nvl(datasource,'all') datasource,
nvl(if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(element_type,'all') element_type,
nvl(list_type,'all') list_type,
nvl(activate_time,'all') activate_time,
count(distinct device_id)  as cart_uv
from
dwd.dwd_vova_rec_report_cart_cause  where pt = '$cur_date'
group by
datasource,
if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),
os_type,
page_code,element_type,
list_type,
activate_time
with cube
) t2 on t1.datasource = t2.datasource and t1.country =t2.country and t1.os_type = t2.os_type and t1.page_code = t2.page_code and t1.list_type =t2.list_type and t1.activate_time =t2.activate_time and t1.element_type = t2.element_type
left join
(
select
nvl(datasource,'all') datasource,
nvl(if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(element_type,'all') element_type,
nvl(list_type,'all') list_type,
nvl(activate_time,'all') activate_time,
count(order_goods_id)  as order_number
from
dwd.dwd_vova_rec_report_order_cause  where pt = '$cur_date'
group by
datasource,
if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),
os_type,
page_code,element_type,
list_type,
activate_time
with cube
) t3 on t1.datasource = t3.datasource and t1.country =t3.country and t1.os_type = t3.os_type and t1.page_code = t3.page_code and t1.list_type = t3.list_type and t1.activate_time =t3.activate_time and t1.element_type = t3.element_type
left join
(
select
nvl(datasource,'all') datasource,
nvl(if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),'all') country,
nvl(os_type,'all') os_type,
nvl(page_code,'all') page_code,
nvl(element_type,'all') element_type,
nvl(list_type,'all') list_type,
nvl(activate_time,'all') activate_time,
count(order_goods_id)  as payed_number,
count(distinct buyer_id) as payed_uv,
sum(gmv) as gmv,
sum(goods_number) as payed_gds_num
from
dwd.dwd_vova_rec_report_pay_cause where pt = '$cur_date'
group by
datasource,
if(country in ('FR','DE','IT','ES','GB','US','PL','BE','RN','CH','TW'),country,'others'),
os_type,
page_code,element_type,
list_type,
activate_time
with cube
) t4 on t1.datasource = t4.datasource and t1.country =t4.country and t1.os_type = t4.os_type and t1.page_code = t4.page_code and t1.list_type = t4.list_type and t1.activate_time =t4.activate_time and t1.element_type = t4.element_type
left join
    (
       select
        sum(fp.shop_price * fp.goods_number + fp.shipping_fee) as total_gmv
        from dwd.dwd_vova_fact_pay fp
        inner join dim.dim_vova_order_goods ddog on ddog.order_goods_id = fp.order_goods_id
        where to_date(fp.pay_time)='${cur_date}' and (fp.from_domain like '%api.vova%' or fp.from_domain like '%api.airyclub%') and fp.platform in ('ios','android')
        and (ddog.order_tag not like '%luckystar_activity_id%' or ddog.order_tag is null)
        ) t5 on 1 = 1
left join tmp.vova_rec_report_tmp t6 on t1.datasource = t6.datasource and t1.country =t6.country and t1.os_type = t6.os_type and t1.page_code = t6.page_code and t1.activate_time =t6.activate_time and t1.element_type = t6.element_type and t1.list_type = t6.list_type

;


insert overwrite table dwb.dwb_vova_rec_active_report  PARTITION (pt = '${cur_date}')
select
/*+ REPARTITION(1) */
to_date('${cur_date}') as event_date,
datasource,
country,
os_type,
page_code,
list_type,
nvl(expres,0) expres,
nvl(clks,0) clks,
nvl(clk_uv,0) clk_uv,
nvl(expre_uv,0) expre_uv,
nvl(cart_uv,0) cart_uv,
nvl(order_number,0) order_number,
nvl(payed_number,0) payed_number,
nvl(payed_uv,0) payed_uv,
nvl(gmv,0) gmv,
activate_time,
nvl(payed_gds_num,0) payed_gds_num,
nvl(total_gmv,0) total_gmv,
nvl(page_uv,0) page_uv,element_type
from tmp.vova_rec_report
where page_code in('theme_activity','theme_activity_ceil_tag','config_active','home_activity_01')
union all
select
/*+ REPARTITION(1) */
to_date('${cur_date}') as event_date,
datasource,
country,
os_type,
page_code,
list_type,
nvl(expres,0) expres,
nvl(clks,0) clks,
nvl(clk_uv,0) clk_uv,
nvl(expre_uv,0) expre_uv,
nvl(cart_uv,0) cart_uv,
nvl(order_number,0) order_number,
nvl(payed_number,0) payed_number,
nvl(payed_uv,0) payed_uv,
nvl(gmv,0) gmv,
activate_time,
nvl(payed_gds_num,0) payed_gds_num,
nvl(total_gmv,0) total_gmv,
nvl(page_uv,0) page_uv,element_type
from tmp.vova_rec_report
where datasource='all' and country='all' and os_type='all' and page_code='all' and list_type='all'  and activate_time = 'all' and element_type = 'all';
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwb_vova_rec_report_active" \
--conf "spark.default.parallelism = 430" \
--conf "spark.sql.shuffle.partitions=430" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.crossJoin.enabled=true" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi




