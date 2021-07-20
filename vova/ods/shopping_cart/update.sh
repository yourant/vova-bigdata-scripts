#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="ods_vova_shopping_cart_log_chenkai_${cur_date}"

###逻辑sql
sql="pdb_fd_order_marketing_data
msck repair table ods_vova_vts.ods_vova_shopping_cart_log_src;

insert overwrite table ods_vova_vts.ods_vova_shopping_cart_log partition(pt='${cur_date}')
select
/*+ REPARTITION(1) */
  CAST(get_json_object(data, '$.user_id') AS BIGINT)             user_id,
  get_json_object(data, '$.session_id')                          session_id,
  CAST(get_json_object(data, '$.goods_id') AS BIGINT)            goods_id,
  CAST(get_json_object(data, '$.sku_id') AS BIGINT)              sku_id,
  get_json_object(data, '$.goods_sn')                            goods_sn,
  CAST(get_json_object(data, '$.market_price') AS decimal(10,2)) market_price,
  CAST(get_json_object(data, '$.shop_price') AS decimal(10,2))   shop_price,
  CAST(get_json_object(data, '$.goods_number') AS int)           goods_number,
  CAST(get_json_object(data, '$.is_real') AS int)                is_real,
  CAST(get_json_object(data, '$.parent_id') AS BIGINT)           parent_id,
  CAST(get_json_object(data, '$.add_time') AS timestamp)         add_time,
  CAST(get_json_object(data, '$.is_sale') AS int)                is_sale
from
  ods_vova_vts.ods_vova_shopping_cart_log_src
where pt ='${cur_date}' and get_json_object(data, '$.kafka_type') = 'insert'
;
"

spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=30" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism=380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
