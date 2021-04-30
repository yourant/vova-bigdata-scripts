#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
spark-sql   --conf "spark.app.name=mlb_vova_search_words_trans" --conf "spark.sql.crossJoin.enabled=true"   --conf "spark.dynamicAllocation.maxExecutors=120"  -e "
insert overwrite table tmp.tmp_vova_search_words_d PARTITION(pt='${cur_date}')
select
/*+ REPARTITION(1) */
lower(trim(regexp_replace(regexp_replace(cc.element_id, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) key_words,language
from dwd.dwd_vova_log_common_click cc
where pt >= '2021-03-06' and cc.buyer_id>0
and cc.element_name = 'search_confirm'
group by lower(trim(regexp_replace(regexp_replace(cc.element_id, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))),language
;
"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi