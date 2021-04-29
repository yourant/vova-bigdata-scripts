#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql
#dependence
#ads_vova_six_rank_mct
#ods_vova_vbai.ods_vova_images_vector
sql="

INSERT OVERWRITE TABLE mlb.mlb_vova_six_mct_flow_support_d PARTITION (pt = '${cur_date}')
SELECT
/*+ REPARTITION(1) */
dvg.goods_id,
dvg.first_cat_id,
nvl(dvg.second_cat_id, 0) AS second_cat_id,
dvg.brand_id,
dvg.goods_name,
iv.img_vec,
nvl(his.impressions, 0) AS impressions,
case
when his.impressions >= 20000 AND gcr < 60 THEN 1
when his.impressions >= 10000 AND gcr < 60 THEN 1
when his.impressions >= 5000 AND sales_order < 1 THEN 1
when his.impressions >= 2000 AND ctr < 0.014 THEN 1
else 0 end AS is_delete
from
ads.ads_vova_six_rank_mct six
INNER JOIN dim.dim_vova_goods dvg on six.mct_id = dvg.mct_id AND six.first_cat_id = dvg.first_cat_id
LEFT JOIN ods_vova_vbai.ods_vova_images_vector iv on dvg.goods_id = iv.goods_id
LEFT JOIN ads.ads_vova_six_mct_flow_support_goods_his his ON his.goods_id = dvg.goods_id AND his.pt = '${cur_date}'
WHERE dvg.is_on_sale = 1
;
"


#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=10" \
--conf "spark.dynamicAllocation.maxExecutors=50" \
--conf "spark.app.name=mlb_vova_six_mct_flow_support_d" \
--conf "spark.default.parallelism = 380" \
--conf "spark.sql.shuffle.partitions=380" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
--conf "spark.sql.inMemoryColumnarStorage.compressed=true" \
--conf "spark.sql.inMemoryColumnarStorage.partitionPruning=true" \
--conf "spark.sql.inMemoryColumnarStorage.batchSize=100000" \
--conf "spark.network.timeout=300" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
