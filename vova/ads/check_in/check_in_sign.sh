#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
echo "cur_date:'${cur_date}'"

sql="
insert overwrite table ads.ads_vova_check_in_sign PARTITION (pt='${cur_date}')
select /*+ REPARTITION(1) */
    if(region_code is null, 'all', region_code)             as region_code,
    if(gmv_stage is null, 'all', cast(gmv_stage as string)) as gmv_stage,
    sign_times_1,
    sign_times_2,
    sign_times_3,
    sign_times_4,
    sign_times_5,
    sign_times_6,
    sign_times_7,
    sign_times_N
from (
         select region_code,
                gmv_stage,
                count(distinct sign_times_1) as sign_times_1,
                count(distinct sign_times_2) as sign_times_2,
                count(distinct sign_times_3) as sign_times_3,
                count(distinct sign_times_4) as sign_times_4,
                count(distinct sign_times_5) as sign_times_5,
                count(distinct sign_times_6) as sign_times_6,
                count(distinct sign_times_7) as sign_times_7,
                count(distinct sign_times_N) as sign_times_N
         from (
                  SELECT if(times = 1, byr.buyer_id, null) as sign_times_1,
                         if(times = 2, byr.buyer_id, null) as sign_times_2,
                         if(times = 3, byr.buyer_id, null) as sign_times_3,
                         if(times = 4, byr.buyer_id, null) as sign_times_4,
                         if(times = 5, byr.buyer_id, null) as sign_times_5,
                         if(times = 6, byr.buyer_id, null) as sign_times_6,
                         if(times = 7, byr.buyer_id, null) as sign_times_7,
                         if(times > 7, byr.buyer_id, null) as sign_times_N,
                         nvl(byr.region_code, 'NA')        as region_code,
                         nvl(y.gmv_stage, 5)               as gmv_stage
                  FROM ods_vova_vts.ods_vova_user_check_in_log ucil
                           inner join dim.dim_vova_buyers byr on ucil.user_id = byr.buyer_id
                      and byr.datasource = 'vova'
                           left join (select *
                                      from ads.ads_vova_buyer_portrait_feature
                                      where pt in (select max(pt) from ads.ads_vova_buyer_portrait_feature)) y
                                     on y.buyer_id = byr.buyer_id
                                         and y.datasource = 'vova'
                  WHERE date (ucil.last_update_time) = '${cur_date}'
     ) tmp
group by region_code,
         gmv_stage with cube
) tmp
;
"

spark-sql \
--conf "spark.app.name=ads_vova_check_in_sign_huachen" \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.broadcastTimeout=600" \
-e "$sql"

if [ $? -ne 0 ];then
  exit 1
fi