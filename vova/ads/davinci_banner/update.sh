#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
pre_date=`date -d "-1 day" +%Y-%m-%d`
fi
sql="
msck repair table ads.ads_davinci_banner;
drop table if exists tmp.ads_davinci_banner;
create table tmp.ads_davinci_banner as
select
/*+ REPARTITION(1) */
t.goods_id,
g.languages_id,
t.img_url
from ads.ads_davinci_banner t
left join dwd.dim_languages g on t.lng = g.languages_code
where t.is_banner =1 and t.pt='$pre_date';
"
spark-sql --conf "spark.app.name=ads_davinci_banner" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ]; then
  exit 1
fi

sqoop export \
-Dorg.apache.sqoop.export.text.dump_data_on_error=true \
-Dmapreduce.job.queuename=important \
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

