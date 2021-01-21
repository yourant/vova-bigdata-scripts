#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
sql="
insert overwrite table dws.dws_vova_goods_core_feature_his partition(pt='${pre_date}')
select
 dg.goods_id as gs_id,
 nvl(tmp_expre.expre_cnt,0) as expre_cnt,
 nvl(nvl(tmp_ord.pay_dau,0)/nvl(tmp_expre.dau,0)*100,0) as cr,
 nvl(nvl(tmp_clk.clk_cnt,0)/nvl(tmp_expre.expre_cnt,0)*100,0) ctr,
 nvl(tmp_ord.gmv,0) as gmv,
 dg.shop_price
 from
 dim.dim_vova_goods dg
 left join
 --曝光数据
 (
 select
 gi.virtual_goods_id as vir_gs_id,
 count(distinct gi.device_id) as dau,
 count(*) as expre_cnt
 from
 dwd.dwd_vova_log_goods_impression gi
 where gi.pt = '${pre_date}' and platform ='mob'
 group by gi.virtual_goods_id) tmp_expre
 on tmp_expre.vir_gs_id = dg.virtual_goods_id
 left join
 -- 点击数数据
 (select
 gc.virtual_goods_id as vir_gs_id,
 count(*) as clk_cnt
 from dwd.dwd_vova_log_goods_click gc
 where gc.pt = '${pre_date}'
 group by gc.virtual_goods_id)tmp_clk
 on dg.virtual_goods_id = tmp_clk.vir_gs_id
 left join
 (select
   goods_id gs_id,
   count(distinct device_id) as pay_dau,
   sum(shop_price*goods_number+shipping_fee) as gmv
   from dwd.dwd_vova_fact_pay fp
   where to_date(order_time) = '${pre_date}' and fp.platform in ('ios','android')
   group by goods_id
 )tmp_ord
 on dg.goods_id = tmp_ord.gs_id;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=20" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dws_vova_goods_core_feature_his" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
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
