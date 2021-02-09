#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "pt=${pre_date}"
sql="
insert overwrite table ads.ads_buyer_portrait_d PARTITION (pt = '${pre_date}')
select
/*+ REPARTITION(4) */
bp.buyer_id user_id,
nvl(bp.pay_cnt_his,0) pay_cnt_his,
nvl(bp.ship_cnt_his,0) ship_cnt_his,
nvl(bp.max_visits_cnt_cw,0) max_visits_cnt_cw,
nvl(bp.price_prefer_1w,'') price_range,
nvl(gs.gmv_stage,0) as gmv_stage
from dws.dws_vova_buyer_portrait bp
left join  ads.ads_buyer_gmv_stage_3m gs
on bp.buyer_id = gs.buyer_id
where bp.pt ='$pre_date' and bp.buyer_id>0 and bp.buyer_id is not null;
"
spark-sql \
--executor-memory 15G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_buyer_portrait_d" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
