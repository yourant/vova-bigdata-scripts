#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
INSERT OVERWRITE TABLE dim.dim_vova_payment
select 'vova' as datasource,
    p.payment_id,
    p.payment_code,
    p.payment_name,
    p.payment_config,
    p.acct_name,
    p.disabled,
    p.is_cod,
    p.is_gc
FROM ods_vova_vts.ods_vova_payment p
"
#如果使用spark-sql运行，则执行hdfs:///
spark-sql  --conf "spark.app.name=dim_vova_payment"  --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi