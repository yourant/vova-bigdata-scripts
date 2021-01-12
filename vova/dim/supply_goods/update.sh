#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dim/dim_vova_supply_goods
###逻辑sql
sql="
insert overwrite table dim.dim_vova_supply_goods
SELECT dg.goods_id,
       tvg.commodity_id,
       tvg.product_id,
       dtnpc.first_cat_name,
       dtnpc.second_cat_name,
       dtnpc.three_cat_name
FROM dim.dim_vova_goods dg
         INNER JOIN ods_gyl_gvg.ods_gyl_goods_info_relation tvg
                   ON dg.virtual_goods_id = tvg.sale_platform_commodity_id
         LEFT JOIN ods_gyl_gnw.ods_gyl_product tnp
                   ON tnp.commodity_id = tvg.commodity_id AND tnp.product_id = tvg.product_id
         LEFT JOIN dim.dim_vova_trigram_nuwa_pdd_category dtnpc ON tnp.cat_id = dtnpc.cat_id
WHERE dg.mct_id IN (26414, 11630, 36655)
;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true"  --conf "spark.dynamicAllocation.minExecutors=20" --conf "spark.dynamicAllocation.initialExecutors=40" --conf "spark.app.name=supply_goods" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
