#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
INSERT OVERWRITE TABLE dim.dim_vova_languages
select 'vova' as datasource,
        languages_id,
        name as languages_name,
        code as languages_code
FROM    ods_vova_themis.ods_vova_languages;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql  --conf "spark.app.name=dim_vova_languages" --conf "spark.sql.parquet.writeLegacyFormat=true"  -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi