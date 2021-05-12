#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
with tmp_union_log(
select device_id ,virtual_goods_id,'expre' as type  from dwd.dwd_vova_log_goods_impression where pt<='2021-05-06' and pt>date_sub('2021-05-06',7) and platform ='mob'  and device_id is not null
union all
select device_id ,virtual_goods_id,'click' as type from dwd.dwd_vova_log_goods_click where pt<='2021-05-06' and pt>date_sub('2021-05-06',7) and platform ='mob'  and device_id is not null
),
tmp_log as (
select
if(dg.brand_id>0,1,0) as is_brand,
dg.first_cat_id,
dg.second_cat_id,
sum(if(tmp_union_log.type='expre',1,0)) as expre_cnt,
sum(if(tmp_union_log.type='click',1,0)) as clk_cnt,
count(distinct(if(tmp_union_log.type='click',tmp_union_log.device_id,null)) ) as clk_uv
from
tmp_union_log
inner join dim.dim_vova_goods dg on tmp_union_log.virtual_goods_id = dg.virtual_goods_id
group by if(dg.brand_id>0,1,0),dg.first_cat_id,dg.second_cat_id
),
tmp_pay(
select
if(dg.brand_id>0,1,0) as is_brand,
dg.first_cat_id,
dg.second_cat_id,
sum(fp.shop_price*fp.goods_number+fp.shipping_fee) gmv,
sum(goods_number) as sales_order
from
(
select
goods_id,
shop_price,
goods_number,
shipping_fee
from
dwd.dwd_vova_fact_pay fp
where date(fp.pay_time)>date_sub('2021-05-06',7) and date(fp.pay_time)<='2021-05-06'
) fp
left join  dim.dim_vova_goods dg on fp.goods_id = dg.goods_id

group by if(dg.brand_id>0,1,0),dg.first_cat_id,dg.second_cat_id
)

insert overwrite table ads.ads_vova_app_group_cat_gcr
select
is_brand,
first_cat_id,
second_cat_id,
case when is_brand =0 and gcr <60 then 60
     when is_brand = 0 and gcr>=60 then gcr
     when is_brand =1 and gcr <120 then 120
     when is_brand =1 and gcr >= 120 then gcr
     end gcr
     from
(select
tmp_log.is_brand,
tmp_log.first_cat_id,
tmp_log.second_cat_id,
nvl(tmp_pay.gmv/tmp_log.clk_uv*tmp_log.clk_cnt/tmp_log.expre_cnt*10000,0) as gcr
from tmp_log
left join tmp_pay on tmp_log.is_brand = tmp_pay.is_brand and tmp_log.first_cat_id = tmp_pay.first_cat_id and tmp_log.second_cat_id = tmp_pay.second_cat_id)
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_app_group_cat_gcr" \
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