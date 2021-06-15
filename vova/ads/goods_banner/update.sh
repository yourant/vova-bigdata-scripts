#!/bin/bash
#指定日期和引擎
pre_date=$1
#默认日期为昨天
if [ ! -n "$1" ]; then
  pre_date=$(date -d "-1 day" +%Y-%m-%d)
fi
emrfs sync s3://vomkt-emr-rec/data/banner_data/tab/ads_banner_image_pre_s3_v2
sql="
set hive.exec.dynamici.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table ads.ads_banner_image_pre partition(pt='${pre_date}')
select
goods_id,
img_id,
img_url,
is_default,
clk_cnt_1m
from
(select
tmp_gs.gs_id as goods_id,
gg.img_url,
gg.img_id,
gg.is_default,
tmp_gs.clk_cnt_1m,
row_number() over(partition by tmp_gs.gs_id order by rand() desc) rk
from
(select
gs_id,
clk_cnt_1m
from
ads.ads_vova_goods_portrait gp
where pt='${pre_date}' and brand_id=0 and clk_cnt_1m>5
and exists (select 1 from ods_vova_vts.ods_vova_goods_gallery gg where gp.gs_id=gg.goods_id and gg.is_default=0 and gg.img_url is not null )
and not exists (select 1 from ads.ads_vova_image_banner t1 where gp.gs_id=t1.goods_id )
)tmp_gs
left join
ods_vova_vts.ods_vova_goods_gallery gg
on tmp_gs.gs_id = gg.goods_id
where img_url is not null and gg.is_default=0)
where rk <=5;

set hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table ads.ads_banner_image_pre_s3_v2 partition(rand)
select
/*+ repartition(1) */
goods_id,
img_id,
img_url,
is_default,
goods_id%30 rand
from
ads.ads_banner_image_pre
where pt='${pre_date}';
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=5" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=ads_banner_image_pre_wcl" \
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
if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=ads_banner_image --from=data --to=arithmetic --jtype=7D --retry=0

if [ $? -ne 0 ];then
  exit 1
fi







