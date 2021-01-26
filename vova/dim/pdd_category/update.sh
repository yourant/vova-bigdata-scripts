#!/usr/bin/env bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dim/dim_vova_trigram_nuwa_pdd_category
### 2.定义执行HQL
sql="
INSERT OVERWRITE TABLE dim.dim_vova_trigram_nuwa_pdd_category
SELECT /*+ REPARTITION(1) */
c.category_id,
c.category_name,
c.category_level,
CASE
    WHEN c.category_level = 1 THEN
        c.category_id
    WHEN c_pri.category_level = 1 THEN
        c_pri.category_id
    WHEN c_ga.category_level = 1 THEN
        c_ga.category_id
    END AS first_category_id,
CASE
    WHEN c.category_level = 1 THEN
        c.category_name
    WHEN c_pri.category_level = 1 THEN
        c_pri.category_name
    WHEN c_ga.category_level = 1 THEN
        c_ga.category_name
    END AS first_category_name,
CASE
    WHEN c.category_level = 2 THEN
        c.category_id
    WHEN c_pri.category_level = 2 THEN
        c_pri.category_id
    WHEN c_ga.category_level = 2 THEN
        c_ga.category_id
    END AS second_category_id,
CASE
    WHEN c.category_level = 2 THEN
        c.category_name
    WHEN c_pri.category_level = 2 THEN
        c_pri.category_name
    WHEN c_ga.category_level = 2 THEN
        c_ga.category_name
    END AS second_category_name,
CASE
    WHEN c.category_level = 3 THEN
        c.category_id
    WHEN c_pri.category_level = 3 THEN
        c_pri.category_id
    WHEN c_ga.category_level = 3 THEN
        c_ga.category_id
    END AS three_category_id,
CASE
    WHEN c.category_level = 3 THEN
        c.category_name
    WHEN c_pri.category_level = 3 THEN
        c_pri.category_name
    WHEN c_ga.category_level = 3 THEN
        c_ga.category_name
    END AS three_category_name,
CASE
    WHEN c.category_level > 1 THEN
        1
    ELSE
        0
    END AS is_leaf
FROM ods_gyl_gnw.ods_gyl_category c
LEFT JOIN ods_gyl_gnw.ods_gyl_category c_pri ON c.parent_id = c_pri.category_id
LEFT JOIN ods_gyl_gnw.ods_gyl_category c_ga ON c_pri.parent_id = c_ga.category_id;
"
#执行hql
spark-sql  --conf "spark.app.name=dim_vova_trigram_nuwa_pdd_category"  --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi
