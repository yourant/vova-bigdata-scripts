#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dwd/dwd_vova_fact_coupon
###逻辑sql
sql="
insert overwrite table dwd.dwd_vova_fact_coupon
select by.datasource,
       oc.coupon_id                     as cpn_id,
       oc.coupon_code                   as cpn_code,
       oc.user_id                       as buyer_id,
       oc.used_order_id                 as order_id,
       from_unixtime(oc.draw_timestamp) as give_time,
       from_unixtime(oc.used_timestamp) as used_time,
       oc.used_times,
       oc.coupon_status                 as cpn_status
from ods_vova_vts.ods_vova_ok_coupon oc
    inner join dim.dim_vova_buyers by on by.buyer_id = oc.user_id;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLeg acyFormat=true"  --conf "spark.app.name=dwd_vova_fact_coupon" --conf "spark.dynamicAllocation.initialExecutors=40" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi