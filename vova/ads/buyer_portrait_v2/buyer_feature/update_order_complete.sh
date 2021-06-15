#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  cur_date=`date +%Y-%m-%d`
fi

sql="
insert overwrite table ads.ads_vova_buyer_order_complate
select
buyer_id,
if(sum(is_order_complete)>=1,1,0) as is_order_complete
from
(select
fp.buyer_id,
if(process_tag='Delivered' or '2021-06-04'> oge.latest_delivery_time,1,0 ) as is_order_complete
from
dwd.dwd_vova_fact_pay fp
left join
dwd.dwd_vova_fact_logistics fl  on fl.order_goods_id = fp.order_goods_id
left join (select rec_id,from_unixtime(extension_info)  as latest_delivery_time from
ods_vova_vts.ods_vova_order_goods_extension where ext_name='latest_delivery_time') oge on fp.order_goods_id = oge.rec_id)
group by
buyer_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_buyer_order_complate" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi