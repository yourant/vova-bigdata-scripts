#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
  pre_date=$(spark-sql -e "msck repair table ads.ads_vova_image_banner;select max(pt) from ads.ads_vova_image_banner")
fi
echo $pre_date
sql="
drop table if exists tmp.ads_davinci_banner;
create table tmp.ads_davinci_banner as
select
/*+ REPARTITION(1) */
t.goods_id,
g.languages_id,
t.url as img_url
from ads.ads_vova_image_banner t
left join dim.dim_vova_languages g on t.language = g.languages_code
where t.is_used =1 and t.pt='$pre_date';
"

spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_davinci_banner" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.autoBroadcastJoinThreshold=-1" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi


sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
--connect jdbc:mysql://rec-recall.cluster-cznqgcwo1pjt.us-east-1.rds.amazonaws.com:3306/rec_recall \
--username dwrecallwriter --password TsLdpZumzovrAvttIqnePCJhIVxZZ7bd \
--table ads_davinci_banner \
--m 1 \
--update-key "goods_id,languages_id" \
--update-mode allowinsert \
--hcatalog-database tmp \
--hcatalog-table ads_davinci_banner \
--fields-terminated-by '\t' \
--columns "goods_id,languages_id,img_url"

#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi