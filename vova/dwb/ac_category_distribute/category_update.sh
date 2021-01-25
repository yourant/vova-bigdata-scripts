#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
###逻辑sql

sql="
INSERT overwrite TABLE dwb.dwb_vova_ac_category_distribute PARTITION ( pt = '${cur_date}' )
SELECT
nvl ( region_code, 'all' ),
nvl ( platform, 'all' ),
nvl ( second_cat_name, 'all' ),
sum( goods_number * shop_price + shipping_fee ) AS gmv,
sum( goods_number ) AS sales_vol
FROM
	(
SELECT
	nvl ( fp.region_code, 'NALL' ) AS region_code,
IF
	( platform IN ( 'pc', 'mob' ), 'web', platform ) AS platform,
	nvl ( dg.second_cat_name, 'NONAME' ) AS second_cat_name,
	fp.goods_number,
	fp.shop_price,
	fp.shipping_fee
FROM
	dwd.dwd_vova_fact_pay fp
	LEFT JOIN dim.dim_vova_goods dg ON fp.goods_id = dg.goods_id
WHERE
	to_date ( fp.pay_time ) = '${cur_date}'
	AND fp.datasource = 'airyclub'
--	AND fp.region_code IN ( 'GB', 'FR', 'DE', 'IT', 'ES' )
	) tmp1
GROUP BY
	region_code,
	platform,
	second_cat_name WITH cube
"
#如果使用spark-sql运行，则执行spark-sql -e
spark-sql \
--executor-memory 8G --executor-cores 1 \
--conf "spark.sql.parquet.writeLegacyFormat=true"  \
--conf "spark.dynamicAllocation.minExecutors=10" \
--conf "spark.dynamicAllocation.initialExecutors=20" \
--conf "spark.app.name=dwb_vova_ac_category_distribute" \
--conf "spark.sql.crossJoin.enabled=true" \
--conf "spark.default.parallelism = 280" \
--conf "spark.sql.shuffle.partitions=280" \
--conf "spark.dynamicAllocation.maxExecutors=100" \
--conf "spark.sql.adaptive.enabled=true" \
--conf "spark.sql.adaptive.join.enabled=true" \
--conf "spark.shuffle.sort.bypassMergeThreshold=10000" \
-e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi