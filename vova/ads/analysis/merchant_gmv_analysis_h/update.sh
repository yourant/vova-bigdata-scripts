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
insert overwrite table ads.ads_vova_merchant_gmv_analysis_h partition(pt,hour)
select
fp.mct_id,
dm.mct_name,
mr.rank as mct_rank,
if(dg.brand_id>0,1,0) as is_brand,
sum(fp.shop_price*fp.goods_number+fp.shipping_fee) as gmv,
'${cur_date}' as pt,
'${pre_hour}' as hour
from
dwd.dwd_vova_fact_pay_h fp
left join dim.dim_vova_merchant dm on fp.mct_id = dm.mct_id
left join dim.dim_vova_goods dg on fp.goods_id = dg.goods_id
left join ads.ads_vova_mct_rank mr on  mr.pt = date_add('${cur_date}',-2) and fp.mct_id = mr.mct_id and fp.first_cat_id = mr.first_cat_id
where date(fp.pay_time) = '${cur_date}' and hour(fp.pay_time) <= ${pre_hour}
group by fp.mct_id,dm.mct_name,mr.rank,if(dg.brand_id>0,1,0);
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_vova_merchant_analysis" \
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
