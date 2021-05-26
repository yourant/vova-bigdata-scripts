#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table  ads.ads_vova_user_analysis partition(pt)
select
/*+ REPARTITION(1) */
fp.order_id,
fp.order_goods_id,
fp.buyer_id,
fp.device_id,
fp.goods_number,
fp.shop_price,
fp.shipping_fee,
pf.reg_gender as gender,
pf.reg_age_group as user_age_group,
pf.reg_channel as main_channel,
date(pay_time) pt
from
dwd.dwd_vova_fact_pay fp
left join ads.ads_vova_buyer_portrait_feature pf on fp.buyer_id= pf.buyer_id and pf.pt='${pre_date}'
where date(fp.pay_time)>date_sub('${pre_date}',15)
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_user_analysis" \
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
