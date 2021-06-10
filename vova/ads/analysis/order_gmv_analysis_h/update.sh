#!/bin/bash
#指定日期和引擎
cur_date=$1
pre_hour=$2
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=$(date "+%Y-%m-%d")
fi
if [ ! -n "$2" ];then
pre_hour=$(date "+%H")
fi

echo "time:${cur_date} ${pre_hour}"

sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_order_gmv_analysis_h partition(pt,hour)
select
t1.country,
t1.order_cnt,
t1.gmv,
nvl((t1.order_cnt - t2.order_cnt)/t2.order_cnt*100,0) as order_cnt_growth_rate,
nvl((t1.gmv - t2.gmv)/t2.gmv*100,0)                   as gmv_growth_rate,
dvr.country_name_cn,
'${cur_date}' as pt,
'${pre_hour}' as hour
from
(select
region_code as country,
count(distinct order_id) as order_cnt,
sum(shop_price*goods_number+shipping_fee) as gmv
from
dwd.dwd_vova_fact_pay_h fp
where date(fp.pay_time) = '${cur_date}' and hour(fp.pay_time) <= ${pre_hour}
group by region_code) t1
left join (select country, max(order_cnt) as order_cnt, max(gmv) as gmv from ads.ads_vova_order_gmv_analysis_h where pt = date_add('${cur_date}',-1) and hour <= ${pre_hour} group by country) t2 on t1.country = t2.country
left join dim.dim_vova_region dvr on t1.country = dvr.country_code and dvr.parent_id=0 and dvr.country_code is not null
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_order_gmv_analysis_h" \
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

if [ $? -ne 0 ];then
  exit 1
fi
