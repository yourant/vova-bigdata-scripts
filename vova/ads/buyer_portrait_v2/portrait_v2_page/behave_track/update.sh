#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table  ads.ads_vova_buyer_behave_track partition(pt='${pre_date}',bpt)
select
/*+ REPARTITION(3) */
db.buyer_id,
tmp1.type,
tmp1.event_type,
tmp1.page_code,
tmp1.element_name,
tmp1.goods_id,
tmp1.event_time,
cast(substr(db.buyer_id,4) as int)%200 as bpt
from
(select
sv.device_id,
'screenview' as type,
null as event_type,
sv.page_code,
null as element_name,
null as goods_id,
cast(sv.collector_tstamp/1000 as timestamp) as event_time
from
dwd.dwd_vova_log_screen_view sv
where sv.pt='${pre_date}' and view_type='show' and page_code != 'app_start'

union all

select
cc.device_id,
'click' as type,
'normal' as event_type,
cc.page_code,
cc.element_name,
null as goods_id,
cast(cc.collector_tstamp/1000 as timestamp) as event_time
from
dwd.dwd_vova_log_common_click cc
where cc.pt='${pre_date}'

union all

select
gc.device_id,
'click' as type,
'goods' as event_type,
gc.page_code,
null as element_name,
dg.goods_id,
cast(gc.collector_tstamp/1000 as timestamp) as event_time
from
dwd.dwd_vova_log_goods_click gc
left join dim.dim_vova_goods dg on gc.virtual_goods_id = dg.virtual_goods_id
where gc.pt='${pre_date}'

union all

select
ld.device_id,
'data' as type,
'null' as event_type,
ld.page_code,
ld.element_name,
dg.goods_id,
cast(ld.collector_tstamp as timestamp) as event_time
from
dwd.dwd_vova_log_data ld
left join dim.dim_vova_goods dg on ld.element_id = dg.virtual_goods_id
where ld.pt='${pre_date}')tmp1
inner join dim.dim_vova_buyers db on tmp1.device_id = db.current_device_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=ads_vova_buyer_behave_track" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=200000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
