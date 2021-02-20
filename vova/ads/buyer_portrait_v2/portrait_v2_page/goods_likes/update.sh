#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_vova_buyer_page_goods_top_behave partition(bpt)
select
/*+ REPARTITION(1) */
tmp1.buyer_id,
db.datasource,
db.current_device_id,
db.email,
tmp1.type,
tmp1.behave_top_array,
cast(substr(tmp1.buyer_id,4) as int)%200 as bpt
from
(select
buyer_id,
'click' as type,
collect_list(concat(gs_id,':',clk_cnt_1m)) as behave_top_array
from
(select
pgl.buyer_id,
pgl.gs_id,
pgl.clk_cnt_1m,
row_number() over(partition by pgl.buyer_id order by pgl.clk_cnt_1m desc) rk
from ads.ads_vova_buyer_portrait_goods_likes pgl
where pgl.pt='${pre_date}' and pgl.clk_cnt_1m>0)
where rk <=20
group by
buyer_id

union all
select
buyer_id,
'collect' as type,
collect_list(concat(gs_id,':',collect_cnt_2w)) as behave_top_array
from
(select
pgl.buyer_id,
pgl.gs_id,
pgl.collect_cnt_2w,
row_number() over(partition by pgl.buyer_id order by pgl.collect_cnt_2w desc) rk
from ads.ads_vova_buyer_portrait_goods_likes pgl
where pgl.pt='${pre_date}' and pgl.collect_cnt_2w>0)
where rk <=20
group by
buyer_id

union all
select
buyer_id,
'order' as type,
collect_list(concat(gs_id,':',ord_cnt_6m)) as behave_top_array
from
(select
pgl.buyer_id,
pgl.gs_id,
pgl.ord_cnt_6m,
row_number() over(partition by pgl.buyer_id order by pgl.ord_cnt_6m desc) rk
from ads.ads_vova_buyer_portrait_goods_likes pgl
where pgl.pt='${pre_date}' and pgl.ord_cnt_6m>0)
where rk <=20
group by
buyer_id)tmp1
left join dim.dim_vova_buyers db on tmp1.buyer_id = db.buyer_id
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--driver-memory 6G \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=200" \
--conf "spark.app.name=ads_vova_buyer_page_goods_top_behave" \
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
