#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

echo "cur_date: $cur_date"

job_name="ads_vova_goods_white_bg_img_a_arc_req8367_chenkai_${cur_date}"

#
sql="
-- a 每日全量
insert overwrite table ads.ads_vova_goods_white_bg_img_a_arc partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  goods_id goods_id,
  sku_id   sku_id,
  img_id  img_id,
  img_url img_url
from
(
  select
    t1.goods_id goods_id,
    vgs.sku_id sku_id,
    vgs.img_id img_id,
    vgg.img_url img_url,
    fp.goods_number goods_number,
    t3.add_cart_uv add_cart_uv,
    row_number() over(partition by t1.goods_id order by fp.goods_number desc, t3.add_cart_uv desc, vgs.sku_id desc) rank
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
  (
    select distinct
      goods_id goods_id,
      sku_id   sku_id,
      img_id  img_id
      -- img_url img_url
    from
      ods_vova_vts.ods_vova_goods_sku
    where is_delete = 0
  ) vgs
  on t1.goods_id = vgs.goods_id
  left join
    ods_vova_vteos.ods_vova_goods_gallery vgg
  on vgs.img_id = vgg.img_id
  left join
  (
    select
      goods_id,
      sku_id,
      sum(goods_number) goods_number
    from
      dwd.dwd_vova_fact_pay
    group by goods_id, sku_id
  ) fp
  on vgs.goods_id = fp.sku_id and vgs.sku_id = fp.sku_id
  left join
  (
    select
      goods_id,
      sku_id,
      count(distinct user_id) add_cart_uv
    from
    ods_vova_vts.ods_vova_shopping_cart_log
    where pt <= '${cur_date}' and pt >= date_sub('${cur_date}', 30)
      and to_date(add_time) <= '${cur_date}' and to_date(add_time) >= date_sub('${cur_date}', 30)
    group by goods_id, sku_id
  ) t3
  on vgs.goods_id = t3.goods_id and vgs.sku_id = t3.sku_id
  where vgs.goods_id is not null and vgg.img_url != ''
) where rank = 1
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
