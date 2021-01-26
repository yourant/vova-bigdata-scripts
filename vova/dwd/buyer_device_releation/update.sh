#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dwd/dwd_vova_fact_buyer_device_releation
sql="
insert overwrite table dwd.dwd_vova_fact_buyer_device_releation PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(200) */
buyer_id,
datasource,
device_id,
app_version,
app_region_code,
region_code,
platform
from
(
select
buyer_id,
device_id,
datasource,
app_version,
app_region_code,
region_code,
platform,
row_number() over (partition by buyer_id,datasource order by pt desc, max_collector_time desc) as rank
from dwd.dwd_vova_fact_start_up su where su.buyer_id > 0 and pt <= '$cur_date'
) su where su.rank = 1
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql  --conf "spark.app.name=dwd_vova_fact_buyer_device_releation"  --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.initialExecutors=60"  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
