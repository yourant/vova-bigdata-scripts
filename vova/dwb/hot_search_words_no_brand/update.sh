#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

###逻辑sql
spark-sql \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.app.name=dwb_vova_hot_search_words_no_brand" \
-e "
INSERT OVERWRITE TABLE dwb.dwb_vova_hot_search_words_no_brand PARTITION (pt = '${cur_date}')
select /*+ REPARTITION(1) */
lower(trim(regexp_replace(regexp_replace(cc.element_id, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) key_words,
sum(if(cc.pt >= date_sub('${cur_date}', 6) and cc.pt <= '${cur_date}',1,0)),
round((sum(if(cc.pt >= date_sub('${cur_date}', 6) and cc.pt <= '${cur_date}',1,0)) - sum(if(cc.pt >= date_sub('${cur_date}', 13) and cc.pt < date_sub('${cur_date}', 6),1,0))) / sum(if(cc.pt >= date_sub('${cur_date}', 13) and cc.pt < date_sub('${cur_date}', 6),1,0)),4)
from dwd.dwd_vova_log_common_click cc
left join (select intercept_keywords from ods_vova_vts.ods_vova_goods_intercept_keywords group by intercept_keywords) dd on
lower(trim(regexp_replace(regexp_replace(cc.element_id, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' '))) = lower(trim(regexp_replace(regexp_replace(dd.intercept_keywords, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))
where cc.pt >= date_sub('${cur_date}', 13) and cc.pt <= '${cur_date}'
and cc.buyer_id > 0
and cc.element_name = 'search_confirm'
group by lower(trim(regexp_replace(regexp_replace(cc.element_id, '\\\\\n|\\\\\t|\\\\\r', ' '),'[\\\s]+',' ')))
order by sum(if(cc.pt >= date_sub('${cur_date}', 6) and cc.pt <= '${cur_date}',1,0)) desc
limit 3000
;
"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

spark-submit \
--deploy-mode client \
--master yarn  \
--num-executors 3 \
--executor-cores 1 \
--executor-memory 8G \
--driver-memory 8G \
--conf spark.app.name=vova_dwb_hot_search_words \
--conf spark.executor.memoryOverhead=2048 \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.eventLog.enabled=false \
--driver-java-options "-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--conf spark.executor.extraJavaOptions="-Dlog4j.configuration=hdfs:/conf/log4j.properties" \
--class com.vova.process.SendData2Interface s3://vomkt-emr-rec/jar/vova-bd/dataprocess/new/vova-db-dataprocess-1.0-SNAPSHOT.jar \
--env prod \
--sql "select words,search_num,increment from dwb.dwb_vova_hot_search_words_no_brand where pt = '${cur_date}'" \
--url " https://vvfeature-t4.vova.com.hk/api/v1/hot-words/add-words" \
--secretKey  "IsJXowR0osUF1sb9abL+VdHlVj9Nw55FzcqeAISAnX1fJYEtdPkRhw8vhZiwv3z9TGLkV7qSjJKHOIorUVfi5ZqOEML3lI0lSL0DR1N8+2ypvEciSEyq0+2hX7xEVjcsBdiiT/AXxWP0F8YaFVWtNuhc5bqnpsaoJTN6xokMcRk=" \
--batchSize 100 \
--id hot_search_words

#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
