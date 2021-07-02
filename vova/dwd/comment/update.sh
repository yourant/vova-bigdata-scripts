#!/bin/bash
#指定日期和引擎
cur_date=$1
#默认日期为昨天
if [ ! -n "$1" ];then
cur_date=`date -d "-1 day" +%Y-%m-%d`
fi
hadoop fs -mkdir s3://bigdata-offline/warehouse/dwd/dwd_vova_fact_comment
###逻辑sql
sql="
insert overwrite table dwd.dwd_vova_fact_comment
SELECT /*+ REPARTITION(15) */
       project_name          AS datasource,
       comment_id,
       goods_id,
       category_id AS cat_id,
       order_goods_id,
       user_id     AS buyer_id,
       title,
       comment,
       rating,
       status,
       post_datetime AS post_time,
       type,
       merchant_id AS mct_id,
       display_order,
       language_id,
       tag,
       if(comment like '%</img>%',1,0) comment_has_img,
       customer_service_rating cs_rating,
       logistics_transportation_rating logistics_rating
FROM ods_vova_vts.ods_vova_goods_comment;
"
#如果使用spark-sql运行，则执行spark-sql --conf "spark.sql.parquet.writeLegacyFormat=true" -e
spark-sql  --conf "spark.app.name=dwd_vova_fact_comment" --conf "spark.sql.parquet.writeLegacyFormat=true" -e "$sql"
#如果脚本失败，则报错
if [ $? -ne 0 ];then
  exit 1
fi