#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

tables=$(spark-sql -e "select table_name from dwb.dwb_vova_dictionary_activity_map where is_on = 1")
sql="with tmp_all as (select biz_type,goods_id from ads.ads_vova_activity_chinese_new_year where pt='${pre_date}'"
for table in $tables
do
sql=$sql" union all select biz_type,goods_id from "$table" where pt='${pre_date}' "
done

sql2="),
tmp_goods as (
select
biz_type,
goods_id
from
tmp_all
group by
biz_type,
goods_id
),
tmp_biz(select
 nvl(nvl(tmp_goods.biz_type,'NONE'),'all') as biz_type,
 nvl(nvl(dg.first_cat_name,'NONE'),'all') as first_cat_name,
 nvl(nvl(dg.second_cat_name,'NONE'),'all') as second_cat_name,
 nvl(nvl(if(dg.brand_id>0,'Y','N'),'NONE'),'all')  as brand,
 count(distinct tmp_goods.goods_id) as goods_cnt,
 avg(dg.shop_price) as avg_price,
 count(distinct if(dg.is_on_sale=1,tmp_goods.goods_id,null)) as on_sale_goods_cnt
 from
tmp_goods
inner join dim.dim_vova_goods dg
on tmp_goods.goods_id = dg.goods_id
group by nvl(tmp_goods.biz_type,'NONE'),nvl(dg.first_cat_name,'NONE'),nvl(dg.second_cat_name,'NONE'),nvl(if(dg.brand_id>0,'Y','N'),'NONE') with cube),
tmp_pay(
select
 nvl(nvl(tmp_goods.biz_type,'NONE'),'all') as biz_type,
 nvl(nvl(dg.first_cat_name,'NONE'),'all') as first_cat_name,
 nvl(nvl(dg.second_cat_name,'NONE'),'all') as second_cat_name,
 nvl(nvl(if(dg.brand_id>0,'Y','N'),'NONE'),'all')  as brand,
 sum(fp.shop_price*fp.goods_number+fp.shipping_fee) as gmv,
 avg((fp.shop_price*fp.goods_number+fp.shipping_fee)/fp.goods_number) as avg_price
from
tmp_goods
inner join dim.dim_vova_goods dg
on tmp_goods.goods_id = dg.goods_id
left join dwd.dwd_vova_fact_pay fp
on tmp_goods.goods_id = fp.goods_id and date(pay_time) = '${pre_date}'
group by nvl(tmp_goods.biz_type,'NONE'),nvl(dg.first_cat_name,'NONE'),nvl(dg.second_cat_name,'NONE'),nvl(if(dg.brand_id>0,'Y','N'),'NONE') with cube
)


insert overwrite table dwb.dwb_vova_activity_biz_behave partition(pt='${pre_date}')
select
tmp_biz.biz_type,
replace(tmp_biz.first_cat_name,'\'','') as first_cat_name,
replace(tmp_biz.second_cat_name,'\'','') as second_cat_name,
tmp_biz.brand,
tmp_biz.goods_cnt,
tmp_biz.avg_price,
tmp_biz.on_sale_goods_cnt,
nvl(tmp_pay.gmv,0) gmv,
nvl(tmp_pay.avg_price,0) sale_avg_price
from
tmp_biz
left join
tmp_pay
on tmp_biz.biz_type = tmp_pay.biz_type
and tmp_biz.first_cat_name = tmp_pay.first_cat_name
and tmp_biz.second_cat_name = tmp_pay.second_cat_name
and tmp_biz.brand = tmp_pay.brand
and tmp_pay.biz_type != 'all'
where tmp_biz.biz_type != 'all'
"
sql_all=$sql$sql2

echo $sql_all



spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_activity_biz_behave" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=200000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql_all"

if [ $? -ne 0 ];then
  exit 1
fi