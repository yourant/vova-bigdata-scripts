#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi

### 2.定义执行HQL
hadoop fs -mkdir s3://bigdata-offline/warehouse/dim/dim_vova_category
sql="
INSERT OVERWRITE TABLE dim.dim_vova_category
SELECT
c.cat_id,
c.cat_name,
c.depth,
CASE
    WHEN c.depth = 1 THEN
        c.cat_id
    WHEN c_pri.depth = 1 THEN
        c_pri.cat_id
    WHEN c_ga.depth = 1 THEN
        c_ga.cat_id
    WHEN c_th.depth = 1 THEN
        c_th.cat_id
    WHEN c_fou.depth = 1 THEN
        c_fou.cat_id
    END AS first_cat_id,
CASE
    WHEN c.depth = 1 THEN
        c.cat_name
    WHEN c_pri.depth = 1 THEN
        c_pri.cat_name
    WHEN c_ga.depth = 1 THEN
        c_ga.cat_name
    WHEN c_th.depth = 1 THEN
        c_th.cat_name
    WHEN c_fou.depth = 1 THEN
        c_fou.cat_name
    END AS first_cat_name,
CASE
    WHEN c.depth = 2 THEN
        c.cat_id
    WHEN c_pri.depth = 2 THEN
        c_pri.cat_id
    WHEN c_ga.depth = 2 THEN
        c_ga.cat_id
    WHEN c_th.depth = 2 THEN
        c_th.cat_id
    WHEN c_fou.depth = 2 THEN
        c_fou.cat_id
    END AS second_cat_id,
CASE
    WHEN c.depth = 2 THEN
        c.cat_name
    WHEN c_pri.depth = 2 THEN
        c_pri.cat_name
    WHEN c_ga.depth = 2 THEN
        c_ga.cat_name
    WHEN c_th.depth = 2 THEN
        c_th.cat_name
    WHEN c_fou.depth = 2 THEN
        c_fou.cat_name
    END AS second_cat_name,
CASE
    WHEN c.depth = 3 THEN
        c.cat_id
    WHEN c_pri.depth = 3 THEN
        c_pri.cat_id
    WHEN c_ga.depth = 3 THEN
        c_ga.cat_id
    WHEN c_th.depth = 3 THEN
        c_th.cat_id
    WHEN c_fou.depth = 3 THEN
        c_fou.cat_id
    END AS three_cat_id,
CASE
    WHEN c.depth = 3 THEN
        c.cat_name
    WHEN c_pri.depth = 3 THEN
        c_pri.cat_name
    WHEN c_ga.depth = 3 THEN
        c_ga.cat_name
    WHEN c_th.depth = 3 THEN
        c_th.cat_name
    WHEN c_fou.depth = 3 THEN
        c_fou.cat_name
    END AS three_cat_name,
CASE
    WHEN c.depth = 4 THEN
        c.cat_id
    WHEN c_pri.depth = 4 THEN
        c_pri.cat_id
    WHEN c_ga.depth = 4 THEN
        c_ga.cat_id
    WHEN c_th.depth = 4 THEN
        c_th.cat_id
    WHEN c_fou.depth = 4 THEN
        c_fou.cat_id
    END AS four_cat_id,
CASE
    WHEN c.depth = 4 THEN
        c.cat_name
    WHEN c_pri.depth = 4 THEN
        c_pri.cat_name
    WHEN c_ga.depth = 4 THEN
        c_ga.cat_name
    WHEN c_th.depth = 4 THEN
        c_th.cat_name
    WHEN c_fou.depth = 4 THEN
        c_fou.cat_name
    END AS four_cat_name,
CASE
    WHEN c.depth > 0 THEN
        1
    ELSE
        0
    END AS is_leaf
FROM ods_vova_vts.ods_vova_category c
LEFT JOIN ods_vova_vts.ods_vova_category c_pri ON c.parent_id = c_pri.cat_id
LEFT JOIN ods_vova_vts.ods_vova_category c_ga ON c_pri.parent_id = c_ga.cat_id
LEFT JOIN ods_vova_vts.ods_vova_category c_th ON c_ga.parent_id = c_th.cat_id
LEFT JOIN ods_vova_vts.ods_vova_category c_fou ON c_th.parent_id = c_fou.cat_id;
"
#执行hql
spark-sql --conf "spark.app.name=dim_vova_vovacategory"  --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi

