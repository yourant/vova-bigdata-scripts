#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
with tmp_gmv_top1000(
select
goods_id,
gmv
from
(select
goods_id,
sum(shop_price*goods_number+shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay
where to_date(pay_time)<='${cur_date}'
and to_date(pay_time)> date_sub('${cur_date}',7)
group by
goods_id)
order by gmv desc
limit 1000)

insert overwrite table dwb.dwb_vova_goods_sn_gmv_top1000_1w partition(pt='${cur_date}')
select
dg.goods_sn,
dg.goods_id,
dg.virtual_goods_id,
dg.first_cat_name,
dg.second_cat_name,
tmp_gmv_1d.gmv as gmv_1d,
tmp_gmv_top1000.gmv as gmv_7d,
brand.brand_name,
dm.mct_name,
sn_min_price.sn_min_shop_price
from
tmp_gmv_top1000
left join dim.dim_vova_goods dg
on tmp_gmv_top1000.goods_id=dg.goods_id
left join ods_vova_vts.ods_vova_brand brand
on dg.brand_id = brand.brand_id
left join dim.dim_vova_merchant dm
on dg.mct_id=dm.mct_id
left join
(
select
goods_id,
sum(shop_price*goods_number+shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay
where to_date(pay_time)='${cur_date}'
group by
goods_id
) tmp_gmv_1d
on tmp_gmv_top1000.goods_id = tmp_gmv_1d.goods_id

left join
(
select
dg.goods_sn,
min(shop_price+shipping_fee) as sn_min_shop_price
from
dim.dim_vova_goods dg
where is_on_sale = 1
group by dg.goods_sn
) sn_min_price
on dg.goods_sn = sn_min_price.goods_sn
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=30" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=dwb_vova_goods_sn_gmv_top1000_1w" \
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
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi