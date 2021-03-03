#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: $cur_date"

job_name="ads_vova_goods_white_bg_img_b_req8367_chenkai_${cur_date}"

#
sql="

insert overwrite table ads.ads_vova_goods_white_bg_img_b_arc partition(pt='${cur_date}')
select /*+ REPARTITION(2) */
  t1.goods_id,
  vgg.img_id,
  vgg.img_url,
  vgg.is_default,
  vgg.img_ctime
from
(
  select /*+ REPARTITION(1) */
    dg.goods_id goods_id
  from
    ads.ads_vova_goods_portrait agp
  left join
    dim.dim_vova_goods dg
  on agp.gs_id = dg.goods_id
  where agp.pt = '${cur_date}' and agp.expre_cnt_1m > 0
    and dg.is_on_sale = 1
    and dg.first_cat_id in (5768,194,5777,5714)
) t1
left join
  ods_vova_vts.ods_vova_goods_gallery vgg
on t1.goods_id = vgg.goods_id
where vgg.img_id is not null and vgg.img_url != '' and vgg.is_delete = 0
;

-- 首次执行
-- insert overwrite table ads.ads_vova_goods_white_bg_img_b_inc partition(pt='${cur_date}')
-- select /*+ REPARTITION(3) */
--   goods_id,
--   img_id,
--   img_url,
--   is_default
-- from
--   ads.ads_vova_goods_white_bg_img_b_arc
-- where pt ='${cur_date}'
--

-- 第二次及之后执行
insert overwrite table ads.ads_vova_goods_white_bg_img_b_inc partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  arc.goods_id,
  arc.img_id,
  arc.img_url,
  arc.is_default
from
(
  select distinct
    goods_id goods_id
  from
    ads.ads_vova_goods_white_bg_img_b_arc
  where pt ='${cur_date}' and to_date(img_ctime) <= '${cur_date}' and to_date(img_ctime) >= date_sub('${cur_date}', 1)
) arc_new
left join
(
  select
    *
  from
    ads.ads_vova_goods_white_bg_img_b_arc
  where pt ='${cur_date}'
) arc
on arc_new.goods_id = arc.goods_id
;
"

#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
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
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
