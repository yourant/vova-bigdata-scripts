#!/bin/bash

cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

###更新goods维度
sql="
insert overwrite table dim.dim_vova_shipping_carrier
select /*+ REPARTITION(1) */ 'vova' as datasource,
       carrier_id,
       carrier_url,
       sc.carrier_name            eng_name,
       sc.display_name            cn_name,

       sc.logistics_provider_id   provider_id,
       sc.logistics_provider_name provider_name,
       logistics_type,
       after_ship_slug,

       sc.carrier_category        carrier_cat,
       vovapost_is_active,
       tracking_source
from ods_vova_vts.ods_vova_shipping_carrier sc;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.app.name=dim_vova_shipping_carrier"  --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

