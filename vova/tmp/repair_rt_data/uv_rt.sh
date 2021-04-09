#!/bin/bash
#指定日期和引擎
cur_date=$1
cur_hour=$2
sql="
insert overwrite table tmp.tmp_uv_rt
select /*+ REPARTITION(1) */
pt cur_date,
hour cur_hour,
'all' datasource,
'all' country,
count(distinct device_id) as uv
from
(
select pt,hour,datasource,geo_country,device_id from dwd.dwd_vova_log_screen_view_arc where pt='2021-04-04' and hour in ('08','09','10','11') and geo_country is not null and geo_country != '' and datasource in ('vova','airyclub')
union all
select pt,hour,datasource,geo_country,device_id from dwd.dwd_vova_log_page_view_arc where pt='2021-04-04' and hour in ('08','09','10','11') and geo_country is not null and geo_country != '' and datasource in ('vova','airyclub') and platform ='mob'
) t
group by pt,hour
union all
select /*+ REPARTITION(1) */
pt cur_date,
hour cur_hour,
'all' datasource,
geo_country country,
count(distinct device_id) as uv
from
(
select pt,hour,datasource,geo_country,device_id from dwd.dwd_vova_log_screen_view_arc where pt='2021-04-04' and hour in ('08','09','10','11') and geo_country is not null and geo_country != '' and datasource in ('vova','airyclub')
union all
select pt,hour,datasource,geo_country,device_id from dwd.dwd_vova_log_page_view_arc where pt='2021-04-04' and hour in ('08','09','10','11') and geo_country is not null and geo_country != '' and datasource in ('vova','airyclub') and platform ='mob'
) t
group by pt,hour,geo_country
union all
select /*+ REPARTITION(1) */
pt cur_date,
hour cur_hour,
datasource,
'all' country,
count(distinct device_id) as uv
from
(
select pt,hour,datasource,geo_country,device_id from dwd.dwd_vova_log_screen_view_arc where pt='2021-04-04' and hour in ('08','09','10','11') and geo_country is not null and geo_country != '' and datasource in ('vova','airyclub')
union all
select pt,hour,datasource,geo_country,device_id from dwd.dwd_vova_log_page_view_arc where pt='2021-04-04' and hour in ('08','09','10','11') and geo_country is not null and geo_country != '' and datasource in ('vova','airyclub') and platform ='mob'
) t
group by pt,hour,datasource
union all
select /*+ REPARTITION(1) */
pt cur_date,
hour cur_hour,
datasource,
geo_country country,
count(distinct device_id) as uv
from
(
select pt,hour,datasource,geo_country,device_id from dwd.dwd_vova_log_screen_view_arc where pt='2021-04-04' and hour in ('08','09','10','11') and geo_country is not null and geo_country != '' and datasource in ('vova','airyclub')
union all
select pt,hour,datasource,geo_country,device_id from dwd.dwd_vova_log_page_view_arc where pt='2021-04-04' and hour in ('08','09','10','11') and geo_country is not null and geo_country != '' and datasource in ('vova','airyclub') and platform ='mob'
) t
group by pt,hour,geo_country,datasource
"
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.sql.output.merge=true"  --conf "spark.app.name=repair_rt_gmv" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-bi.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/themis \
--username bdwriter --password Dd7LvXRPDP4iIJ7FfT8e \
--m 1 \
--table rpt_uv_rt \
--update-key "datasource,country,cur_date,cur_hour" \
--update-mode allowinsert \
--hcatalog-database tmp \
--hcatalog-table tmp_uv_rt \
--fields-terminated-by '\001'