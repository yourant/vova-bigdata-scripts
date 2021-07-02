#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
with tmp_union_log(
select if(datasource  in ('vova','airyclub'),datasource,'vova_app_site') as datasource,device_id ,virtual_goods_id,'expre' as type  from dwd.dwd_vova_log_goods_impression where pt<='${pre_date}' and pt>date_sub('${pre_date}',7) and platform ='mob'  and device_id is not null and datasource is not null
union all
select  if(datasource  in ('vova','airyclub'),datasource,'vova_app_site') as datasource,device_id ,virtual_goods_id,'click' as type from dwd.dwd_vova_log_goods_click where pt<='${pre_date}' and pt>date_sub('${pre_date}',7) and platform ='mob'  and device_id is not null and datasource is not null
),
tmp_log as (
select
nvl(tmp_union_log.datasource,'all') as datasource,
dg.goods_id,
dg.brand_id,
dg.first_cat_id,
dg.second_cat_id,
sum(if(tmp_union_log.type='expre',1,0)) as expre_cnt,
sum(if(tmp_union_log.type='click',1,0)) as clk_cnt,
count(distinct(if(tmp_union_log.type='click',tmp_union_log.device_id,null)) ) as clk_uv
from
tmp_union_log
inner join dim.dim_vova_goods dg on tmp_union_log.virtual_goods_id = dg.virtual_goods_id
group by dg.goods_id,dg.brand_id,dg.first_cat_id,dg.second_cat_id,tmp_union_log.datasource
grouping sets(
(dg.goods_id,dg.brand_id,dg.first_cat_id,dg.second_cat_id,tmp_union_log.datasource),
(dg.goods_id,dg.brand_id,dg.first_cat_id,dg.second_cat_id)
)
),
tmp_pay(
select
nvl(fp.datasource,'all') as datasource,
fp.goods_id,
sum(fp.shop_price*fp.goods_number+fp.shipping_fee) gmv,
sum(goods_number) as sales_order
from
(
select
if(fp.datasource like '%vova%','vova',if(fp.datasource like '%airyclub%','airyclub','vova_app_site')) as datasource,
goods_id,
shop_price,
goods_number,
shipping_fee
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time)>date_sub('${pre_date}',7) and date(fp.pay_time)<='${pre_date}' and fp.platform in ('ios','android')
) fp
left join  dim.dim_vova_goods dg on fp.goods_id = dg.goods_id

group by fp.goods_id,fp.datasource
grouping sets(
(fp.goods_id),
(fp.goods_id,fp.datasource)
)
),

tmp_date(
select
tmp_log.goods_id,
tmp_log.datasource,
if(tmp_log.brand_id>0,1,0) as is_brand,
tmp_log.first_cat_id,
nvl(tmp_log.second_cat_id,0) as second_cat_id,
nvl(tmp_log.expre_cnt,0) as impressions,
nvl(tmp_log.clk_cnt,0) as clicks,
nvl(tmp_log.clk_uv,0) as users,
nvl(tmp_pay.sales_order,0) as sales_order,
nvl(tmp_pay.gmv,0) as gmv,
nvl(tmp_log.clk_cnt/tmp_log.expre_cnt,0) as ctr,
nvl(tmp_pay.gmv/tmp_log.clk_uv*tmp_log.clk_cnt/tmp_log.expre_cnt*10000,0) as gcr
from tmp_log
left join tmp_pay on tmp_log.goods_id = tmp_pay.goods_id and tmp_log.datasource =  tmp_pay.datasource
)

insert overwrite table ads.ads_vova_app_group_test_goods partition(pt='${pre_date}')
select
datasource,
'mob' as platform,
'ALL' as region_codes,
'' as region_ids,
goods_id,
users,
clicks,
impressions,
sales_order,
1 as is_compliance,
'computer' as employee_name,
'' as select_cat_channel,
gmv,
ctr,
gcr,
0 as test_status,
0 as test_result,
current_timestamp() as create_time,
current_timestamp() as last_update_time
from
tmp_date t1
where t1.datasource = 'vova_app_site' and impressions <= 1000 and gmv >0 and ctr >= 0.03 and gcr>60
-- and exists
-- vova 数据判断
-- (
-- select
-- 1
-- from tmp_date t2 where datasource = 'vova' and is_brand = 1 and sales_order >= 7 and ctr > 0.02 and t1.goods_id = t2.goods_id
-- )
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_app_group_test_goods" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

spark-submit --master yarn \
--deploy-mode client \
--driver-memory 8G \
--executor-memory 8G \
--conf spark.dynamicAllocation.maxExecutors=100 \
--name "ads_vova_app_group_test_goods_export" \
--class com.vova.bigdata.sparkbatch.dataprocess.ads.AppGroupGoodsTest s3://vomkt-emr-rec/jar/vova-bigdata/vova-bigdata-sparkbatch/vova-bigdata-sparkbatch-1.0-SNAPSHOT.jar \
--envFile prod --pt ${pre_date} --tableName test_goods_behave

if [ $? -ne 0 ]; then
  exit 1
fi