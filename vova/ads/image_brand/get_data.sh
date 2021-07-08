#!/bin/bash
echo "start_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`

#指定日期和引擎
cur_date=$1

#默认日期为昨天
if [ ! -n "$1" ];then
  cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
# 当天日期
echo "cur_date: ${cur_date}"

table_suffix=`date -d "${cur_date}" +%Y%m%d`
echo "table_suffix: ${table_suffix}"

job_name="ads_vova_no_brand_goods_img_get_data_req9531_chenkai_${cur_date}"

###逻辑sql
# 取数: 相似图片组中历史有销量的非brand商品
sql="
insert overwrite table ads.ads_vova_no_brand_goods_img partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
  t1.goods_id,
  vgg.img_id,
  vgg.img_url
from
(
  select
    distinct goods_id, group_id
  from
  (
    select
      nvl(rgps.group_id, dg.goods_id) group_id,
      dg.goods_id,
      dg.shop_price,
      row_number() over(partition by if(rgps.group_id is not null, rgps.group_id, dg.goods_id) order by dg.shop_price) row
    from
      dim.dim_vova_goods dg
    left join
      ods_vova_vbts.ods_vova_rec_gid_pic_similar rgps
    on rgps.goods_id = dg.goods_id
    where dg.brand_id = 0
      and dg.is_on_sale = 1
      and dg.add_time <= date_sub('${cur_date}', 7)
  )
  where row = 1
) t1
left join
(
  select
    distinct if(rgps.group_id is not null, rgps.group_id, t1.goods_id) group_id
  from
  (
    select distinct
      goods_id
    from
      ads.ads_vova_no_brand_goods_img
    where pt < '${cur_date}'
  ) t1 
  left join
    ods_vova_vbts.ods_vova_rec_gid_pic_similar rgps
  on t1.goods_id = rgps.goods_id
) t_arc
on t_arc.group_id = t1.group_id
left join
  ods_vova_vteos.ods_vova_goods_gallery vgg
on t1.goods_id = vgg.goods_id
where vgg.img_id is not null
  and vgg.img_url != ''
  and vgg.is_delete = 0
  and t_arc.group_id is null
;
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 6G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=${job_name}" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism=300" \
--conf "spark.sql.shuffle.partitions=300" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
echo "${job_name} end_time:"  `date +"%Y-%m-%d %H:%M:%S" -d "8 hour"`
