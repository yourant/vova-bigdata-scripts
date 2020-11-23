#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
sql="
insert overwrite table dim.dim_vova_coupon
select
       byr.datasource,
       oc.coupon_id                   as cpn_id,
       oc.coupon_code                 as cpn_code,
       oc.coupon_config_id            as cpn_cfg_id,
       occ.coupon_config_coupon_type  as cpn_cfg_type,
       occ.coupon_config_value        as cpn_cfg_val,
       occ.coupon_config_apply_type   as cpn_use_type,
       occ.coupon_config_type_id      as cpn_cfg_type_id,
       occt.config_type_name          as cpn_cfg_type_name,
       oc.user_id                     as buyer_id,
       from_unixtime(oc.coupon_ctime) as cpn_create_time,
       oc.extend_day,
       oc.can_use_times
from ods_vova_themis.ods_vova_ok_coupon oc
         inner join dim.dim_vova_buyers byr on byr.buyer_id = oc.user_id
         inner join ods_vova_themis.ods_vova_ok_coupon_config occ on oc.coupon_config_id = occ.coupon_config_id
         inner join ods_vova_themis.ods_vova_ok_coupon_config_type occt on occt.coupon_config_type_id = occ.coupon_config_type_id
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.app.name=dim_vova_coupon"  --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=30" --conf "spark.dynamicAllocation.initialExecutors=60" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
