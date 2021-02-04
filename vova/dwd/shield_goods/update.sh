#!/bin/bash
hadoop fs -mkdir s3://bigdata-offline/warehouse/dwd/dwd_vova_fact_shield_goods
#指定日期和引擎
sql="
insert overwrite table dwd.dwd_vova_fact_shield_goods
select /*+ REPARTITION(200) */
goods_id,
region_id,
mct_id,
shield_type,
create_time
from  (select
    key_id goods_id,
    region_id,
    merchant_id mct_id,
    key_type shield_type,
    create_time
from ods_vova_vts.ods_vova_merchant_region
where key_type = 'goods'
union all
select
    g.goods_id,
    m.region_id,
    m.key_id mct_id,
    m.key_type shield_type,
    m.create_time
from ods_vova_vts.ods_vova_goods g
left join ods_vova_vts.ods_vova_merchant_region m on g.merchant_id = m.key_id
where key_type = 'merchant') t
group by goods_id,
region_id,
mct_id,
shield_type,
create_time
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 4G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=dwd_vova_fact_shield_goods" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
--conf "spark.sql.autoBroadcastJoinThreshold=31457280" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi
