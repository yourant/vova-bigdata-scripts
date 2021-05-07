#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
INSERT OVERWRITE TABLE ads.ads_vova_sale_goods_3m PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(5) */
fp.goods_id,
sum(fp.goods_number) AS sales_order
FROM dwd.dwd_vova_fact_pay fp
WHERE DATE(fp.pay_time) >= date_sub('${cur_date}', 89)
  AND DATE(fp.pay_time) <= '${cur_date}'
GROUP BY fp.goods_id
;
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=10" \
--conf "spark.dynamicAllocation.maxExecutors=50" \
--conf "spark.app.name=ads_vova_sale_goods_3m" \
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
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
