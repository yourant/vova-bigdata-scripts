#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
INSERT OVERWRITE TABLE dim.dim_vova_region
select /*+ REPARTITION(1) */
'vova' as datatsource,
r1.region_id,
r1.parent_id,
       case
           when r1.region_type = 0 then r1.region_id
           when r2.region_type = 0 then r2.region_id
           when r3.region_type = 0 then r3.region_id
           when r4.region_type = 0 then r4.region_id
           end as country_id,
       case
           when r1.region_type = 0 then r1.region_code
           when r2.region_type = 0 then r2.region_code
           when r3.region_type = 0 then r3.region_code
           when r4.region_type = 0 then r4.region_code
           end as country_code,
       case
           when r1.region_type = 0 then r1.region_name
           when r2.region_type = 0 then r2.region_name
           when r3.region_type = 0 then r3.region_name
           when r4.region_type = 0 then r4.region_name
           end as country_name,
       case
           when r1.region_type = 0 then r1.region_name_cn
           when r2.region_type = 0 then r2.region_name_cn
           when r3.region_type = 0 then r3.region_name_cn
           when r4.region_type = 0 then r4.region_name_cn
           end as country_name_cn,
       case
           when r1.region_type = 1 then r1.region_id
           when r2.region_type = 1 then r2.region_id
           when r3.region_type = 1 then r3.region_id
           when r4.region_type = 1 then r4.region_id
           end as first_region_id,
       case
           when r1.region_type = 1 then r1.region_name
           when r2.region_type = 1 then r2.region_name
           when r3.region_type = 1 then r3.region_name
           when r4.region_type = 1 then r4.region_name
           end as first_region_name,
       case
           when r1.region_type = 1 then r1.region_name_cn
           when r2.region_type = 1 then r2.region_name_cn
           when r3.region_type = 1 then r3.region_name_cn
           when r4.region_type = 1 then r4.region_name_cn
           end as first_region_name_cn,
       case
           when r1.region_type = 2 then r1.region_id
           when r2.region_type = 2 then r2.region_id
           when r3.region_type = 2 then r3.region_id
           when r4.region_type = 2 then r4.region_id
           end as second_region_id,
       case
           when r1.region_type = 2 then r1.region_name
           when r2.region_type = 2 then r2.region_name
           when r3.region_type = 2 then r3.region_name
           when r4.region_type = 2 then r4.region_name
           end as second_region_name,
       case
           when r1.region_type = 2 then r1.region_name_cn
           when r2.region_type = 2 then r2.region_name_cn
           when r3.region_type = 2 then r3.region_name_cn
           when r4.region_type = 2 then r4.region_name_cn
           end as second_region_name_cn,
       case
           when r1.region_type = 3 then r1.region_id
           when r2.region_type = 3 then r2.region_id
           when r3.region_type = 3 then r3.region_id
           when r4.region_type = 3 then r4.region_id
           end as area_id,
       case
           when r1.region_type = 3 then r1.region_name
           when r2.region_type = 3 then r2.region_name
           when r3.region_type = 3 then r3.region_name
           when r4.region_type = 3 then r4.region_name
           end as area_name,
       case
           when r1.region_type = 3 then r1.region_code
           when r2.region_type = 3 then r2.region_code
           when r3.region_type = 3 then r3.region_code
           when r4.region_type = 3 then r4.region_code
           end as area_code
from ods_vova_vts.ods_vova_region r1
         left join
     ods_vova_vts.ods_vova_region r2 on r1.parent_id = r2.region_id
         left join
     ods_vova_vts.ods_vova_region r3 on r2.parent_id = r3.region_id
         left join
     ods_vova_vts.ods_vova_region r4 on r3.parent_id = r4.region_id;
"
spark-sql --conf "spark.app.name=dim_vova_region" --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
