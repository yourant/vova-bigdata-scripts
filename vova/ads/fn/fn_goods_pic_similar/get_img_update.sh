#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
INSERT OVERWRITE TABLE ads.fn_ads_img_snapshot PARTITION (pt = '${cur_date}')
SELECT /*+ REPARTITION(1) */
    fdg.goods_id,
    fgg.img_id,
    fgg.img_url,
    fgg.img_original,
    fgg.last_update_time
FROM dim.dim_zq_goods fdg
         INNER JOIN
     (
         SELECT t1.img_original,
                t1.goods_id,
                t1.img_id,
                t1.last_update_time,
                t1.img_url
         FROM (
                  SELECT if(gg.img_original like '/spider%', concat('https://supply-img.vova.com.hk', gg.img_original), gg.img_original) as img_original,
                         gg.goods_id,
                         gg.img_id,
                         gg.img_url,
                         gg.last_update_time,
                         row_number() OVER (PARTITION BY gg.goods_id ORDER BY gg.last_update_time DESC ) rank
                  FROM ods_zq_zsp.ods_zq_goods_gallery gg
                  WHERE gg.is_delete = 0
                    AND gg.is_default = 1
              ) t1
         WHERE t1.rank = 1
     ) fgg ON fgg.goods_id = fdg.goods_id
WHERE fdg.is_on_sale = 1
  AND fdg.datasource = 'florynight'
;

insert overwrite table ads.fn_ads_img_snapshot_arc partition(pt='${cur_date}')
select /*+ REPARTITION(1) */
t1.goods_id,
t1.img_id,
t1.img_original
from
(
select
goods_id,
img_id,
img_original
from
ads.fn_ads_img_snapshot
where pt = '${cur_date}'
) t1
left join
(
select
goods_id,
img_id,
img_original
from
ads.fn_ads_img_vector
where pt = date_sub('${cur_date}', 1)
) t2 on t1.goods_id = t2.goods_id
where t1.img_id != t2.img_id or t1.img_original != t2.img_original or t2.goods_id is null
;

"

spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=fn_ads_goods_score_1d" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

sh /mnt/vova-bigdata-scripts/common/job_message_put.sh --jname=fn_images_original_20201113 --from=data --to=arithmetic --jtype=1D --retry=0

if [ $? -ne 0 ];then
  exit 1
fi



