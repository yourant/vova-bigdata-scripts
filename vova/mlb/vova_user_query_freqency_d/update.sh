#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.dynamicAllocation.maxExecutors=150" \
--conf "spark.app.name=mlb_vova_user_query_freqency_d" \
-e "
insert overwrite table mlb.mlb_vova_user_query_freqency_d  PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(10) */
lower(trim(regexp_replace(regexp_replace(cc.element_id, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) key_words,
b.translation_query,
count(1) pv,
count(distinct device_id) uv,
count(distinct device_id, cc.pt) pt_uv, --同一个用户不同天搜索算多次
count(distinct device_id, session_id) session_uv --同一个用户不同session搜索算多次
from dwd.dwd_vova_log_common_click cc
left join mlb.mlb_vova_user_query_translation_d b on lower(trim(regexp_replace(regexp_replace(cc.element_id, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) = b.clk_from
where cc.pt >= date_sub('${cur_date}', 30)
and cc.buyer_id > 0
and cc.element_name = 'search_confirm'
group by lower(trim(regexp_replace(regexp_replace(cc.element_id, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),b.translation_query
"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
