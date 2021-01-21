#!/usr/bin/env bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

### 2.定义执行HQL
sql="
INSERT OVERWRITE TABLE dim.dim_zq_site
SELECT
/*+ REPARTITION(1) */
project_name as datasource,
domain_group
from
ods_zq_zsp.ods_zq_site ss
where ss.status = 1
and ss.type = 1
group by project_name, domain_group

"
#执行hql
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

